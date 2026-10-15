"""Data quality validation engine for RetailPulse."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Sequence, Tuple, Union

from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as F
from pyspark.sql.types import StringType

from retailpulse.common.logging import get_logger
from retailpulse.validation.rules import RuleType, ValidationRule

logger = get_logger("validation.engine")


def _normalize_rules(
    rules: Union[
        Sequence[ValidationRule],
        Sequence[Dict[str, Any]],
        Dict[str, Any],
    ],
) -> List[ValidationRule]:
    """Convert input rules into a list of ValidationRule instances."""
    if isinstance(rules, dict):
        if "rules" in rules and isinstance(rules["rules"], list):
            raw_rules = rules["rules"]
        else:
            raw_rules = [rules]
    else:
        raw_rules = list(rules)

    normalized: List[ValidationRule] = []
    for item in raw_rules:
        if isinstance(item, ValidationRule):
            normalized.append(item)
        elif isinstance(item, dict):
            normalized.append(ValidationRule.from_dict(item))
        else:
            raise TypeError(
                f"Unsupported rule format: {type(item)}. Expected ValidationRule or dict."
            )
    return normalized


def validate(
    df: DataFrame,
    rules: Union[
        Sequence[ValidationRule],
        Sequence[Dict[str, Any]],
        Dict[str, Any],
    ],
) -> Tuple[DataFrame, DataFrame]:
    """Validate a PySpark DataFrame against a suite of data quality rules.

    Args:
        df: Input PySpark DataFrame to validate.
        rules: Sequence of ValidationRule instances or rule dictionaries.

    Returns:
        Tuple[DataFrame, DataFrame]:
            - good_df: Rows that passed all validation checks (original schema).
            - bad_df: Rows that failed at least one rule, enriched with
                      'rule_failed' (comma-separated rule names) and
                      'reason' (semicolon-separated failure explanations).

    Raises:
        ValueError: If a rule references a column that does not exist in `df`.
    """
    rule_list = _normalize_rules(rules)
    # Evaluate content checks before choosing the survivor of duplicate keys.
    rule_list.sort(key=lambda rule: rule.rule_type == RuleType.UNIQUE)
    original_cols = df.columns
    spark = df.sparkSession

    # If rules list is empty, all rows pass
    if not rule_list:
        logger.info("Validation called with 0 rules; all rows accepted.")
        empty_bad_schema = df.schema.add("rule_failed", StringType(), False).add(
            "reason", StringType(), False
        )
        empty_bad_df = spark.createDataFrame([], empty_bad_schema)
        return df, empty_bad_df

    # Verify that columns referenced by rules exist in DataFrame
    available_cols = set(df.columns)
    for rule in rule_list:
        cols_to_check = rule.columns or ([rule.column] if rule.column else [])
        for col in cols_to_check:
            if col not in available_cols:
                raise ValueError(
                    f"Column '{col}' specified in rule '{rule.name}' "
                    f"does not exist in DataFrame columns: {df.columns}"
                )

    working_df = df
    temp_cols: List[str] = []
    rule_evaluations: List[Tuple[Any, Any, str]] = []

    # Build evaluation expressions for each rule
    for idx, rule in enumerate(rule_list):
        rtype = rule.rule_type
        col_name = rule.column
        params = rule.params

        if rtype == RuleType.NOT_NULL:
            assert col_name is not None
            inv_cond = F.col(col_name).isNull() | (F.trim(F.col(col_name).cast("string")) == "")
            reason_expr = F.lit(f"Column '{col_name}' is null")

        elif rtype == RuleType.RANGE:
            assert col_name is not None
            min_val = params.get("min_val")
            max_val = params.get("max_val")
            inc_min = params.get("inclusive_min", True)
            inc_max = params.get("inclusive_max", True)

            range_conds = []
            is_temporal = str(max_val).lower() in ("now", "today", "current_date", "current_timestamp")
            value = F.to_timestamp(F.col(col_name)) if is_temporal else F.col(col_name).cast("double")
            if min_val is not None:
                min_expr = (
                    (value < min_val)
                    if inc_min
                    else (value <= min_val)
                )
                range_conds.append(min_expr)

            is_future_check = False
            if max_val is not None:
                if str(max_val).lower() in (
                    "now",
                    "today",
                    "current_date",
                    "current_timestamp",
                ):
                    is_future_check = True
                    max_expr = F.to_timestamp(F.col(col_name)) > F.current_timestamp()
                else:
                    max_expr = (
                        (value > max_val)
                        if inc_max
                        else (value >= max_val)
                    )
                range_conds.append(max_expr)

            # Combine range condition
            if range_conds:
                combined_range = range_conds[0]
                for rc in range_conds[1:]:
                    combined_range = combined_range | rc
                inv_cond = value.isNull() | combined_range
            else:
                inv_cond = value.isNull()
            if not is_temporal:
                inv_cond = inv_cond | F.isnan(value) | (F.abs(value) == F.lit(float("inf")))

            if is_future_check:
                reason_expr = F.concat(
                    F.lit(f"Column '{col_name}' ("),
                    F.coalesce(F.col(col_name).cast("string"), F.lit("null")),
                    F.lit(") is in the future"),
                )
            else:
                reason_expr = F.concat(
                    F.lit(f"Column '{col_name}' ("),
                    F.coalesce(F.col(col_name).cast("string"), F.lit("null")),
                    F.lit(f") out of range (min={min_val}, max={max_val})"),
                )

        elif rtype in (RuleType.REGEX, RuleType.FORMAT):
            assert col_name is not None
            pattern = params.get("pattern", "")
            inv_cond = F.col(col_name).isNull() | (
                ~F.col(col_name).cast("string").rlike(pattern)
            )
            reason_expr = F.concat(
                F.lit(f"Column '{col_name}' ("),
                F.coalesce(F.col(col_name).cast("string"), F.lit("null")),
                F.lit(f") does not match pattern '{pattern}'"),
            )

        elif rtype == RuleType.ALLOWED_VALUES:
            assert col_name is not None
            allowed = params.get("values", [])
            inv_cond = F.col(col_name).isNull() | (~F.col(col_name).isin(allowed))
            reason_expr = F.concat(
                F.lit(f"Column '{col_name}' ("),
                F.coalesce(F.col(col_name).cast("string"), F.lit("null")),
                F.lit(f") is not in allowed values: {allowed}"),
            )

        elif rtype == RuleType.UNIQUE:
            unique_cols = rule.columns or ([col_name] if col_name else [])
            helper_col = f"__rp_rn_{idx}"
            temp_cols.append(helper_col)

            keep = params.get("keep", "first")
            if keep not in ("first", "last"):
                raise ValueError("unique keep must be 'first' or 'last'")
            invalid_score = F.lit(0)
            for condition, _, _ in rule_evaluations:
                invalid_score = invalid_score + F.when(condition, 1).otherwise(0)
            ordering = [invalid_score.asc()]
            for name in params.get("order_by", []):
                if name in original_cols:
                    ordering.append(F.col(name).desc_nulls_last() if keep == "last" else F.col(name).asc_nulls_last())
            # Stable tie-breaking independent of Spark partition/input ordering.
            ordering.extend(
                F.col(c).cast("string").desc_nulls_last() if keep == "last"
                else F.col(c).cast("string").asc_nulls_last()
                for c in sorted(original_cols)
            )
            window_spec = Window.partitionBy(
                [F.col(c) for c in unique_cols]
            ).orderBy(*ordering)
            working_df = working_df.withColumn(
                helper_col, F.row_number().over(window_spec)
            )

            inv_cond = F.col(helper_col) > 1
            reason_expr = F.lit(
                f"Duplicate record for key: ({', '.join(unique_cols)})"
            )

        elif rtype == RuleType.REFERENTIAL:
            assert col_name is not None
            ref_df = params.get("ref_df")
            ref_col = params.get("ref_column", col_name)
            ref_values = params.get("ref_values")

            if ref_values is not None:
                inv_cond = F.col(col_name).isNull() | (
                    ~F.col(col_name).isin(list(ref_values))
                )
                reason_expr = F.concat(
                    F.lit(f"Column '{col_name}' ("),
                    F.coalesce(F.col(col_name).cast("string"), F.lit("null")),
                    F.lit(f") foreign key not found in reference values"),
                )
            elif ref_df is not None:
                helper_col = f"__rp_ref_{idx}"
                temp_cols.append(helper_col)

                ref_keys = ref_df.select(
                    F.col(ref_col).alias(helper_col)
                ).distinct()
                working_df = working_df.join(
                    ref_keys,
                    F.col(col_name) == F.col(helper_col),
                    how="left",
                )

                inv_cond = F.col(col_name).isNull() | F.col(helper_col).isNull()
                reason_expr = F.concat(
                    F.lit(f"Column '{col_name}' ("),
                    F.coalesce(F.col(col_name).cast("string"), F.lit("null")),
                    F.lit(f") foreign key does not exist in reference {ref_col}"),
                )
            else:
                raise ValueError(
                    f"Referential rule '{rule.name}' requires either 'ref_df' or 'ref_values'."
                )

        else:
            raise ValueError(f"Unsupported validation rule type: '{rtype}'")

        rule_evaluations.append((inv_cond, reason_expr, rule.name))

    # Aggregate all failing rules and reasons per row
    failed_rules_expr = F.filter(
        F.array(
            [
                F.when(inv_cond, F.lit(rule_name)).otherwise(F.lit(None))
                for inv_cond, _, rule_name in rule_evaluations
            ]
        ),
        lambda x: x.isNotNull(),
    )

    reasons_expr = F.filter(
        F.array(
            [
                F.when(inv_cond, reason_expr).otherwise(F.lit(None))
                for inv_cond, reason_expr, _ in rule_evaluations
            ]
        ),
        lambda x: x.isNotNull(),
    )

    annotated = working_df.withColumn(
        "_failed_rules", failed_rules_expr
    ).withColumn("_reasons", reasons_expr)

    is_bad_cond = F.size(F.col("_failed_rules")) > 0

    good_df = annotated.filter(~is_bad_cond).select(original_cols)
    bad_df = (
        annotated.filter(is_bad_cond)
        .withColumn("rule_failed", F.array_join(F.col("_failed_rules"), ", "))
        .withColumn("reason", F.array_join(F.col("_reasons"), "; "))
        .select(original_cols + ["rule_failed", "reason"])
    )

    return good_df, bad_df
