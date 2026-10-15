"""Rule definitions and source presets for the RetailPulse validation engine."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Union

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class RuleType(str, Enum):
    """Supported validation rule types."""

    NOT_NULL = "not_null"
    RANGE = "range"
    REGEX = "regex"
    FORMAT = "format"
    UNIQUE = "unique"
    ALLOWED_VALUES = "allowed_values"
    REFERENTIAL = "referential"


@dataclass
class ValidationRule:
    """Configurable data quality validation rule."""

    name: str
    rule_type: str
    column: Optional[str] = None
    columns: Optional[List[str]] = None
    params: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        # Normalize rule_type
        if isinstance(self.rule_type, RuleType):
            self.rule_type = self.rule_type.value
        else:
            self.rule_type = str(self.rule_type).lower()

        # If columns given as string, convert to list
        if isinstance(self.columns, str):
            self.columns = [self.columns]
        elif self.columns is not None:
            self.columns = list(self.columns)

        # Ensure either column or columns is set
        if not self.column and not self.columns:
            raise ValueError(
                f"Rule '{self.name}' must specify either 'column' or 'columns'."
            )

    @classmethod
    def not_null(
        cls,
        column: str,
        name: Optional[str] = None,
    ) -> ValidationRule:
        """Create a NOT NULL rule."""
        rule_name = name or f"not_null:{column}"
        return cls(name=rule_name, rule_type=RuleType.NOT_NULL, column=column)

    @classmethod
    def range(
        cls,
        column: str,
        min_val: Any = None,
        max_val: Any = None,
        inclusive_min: bool = True,
        inclusive_max: bool = True,
        name: Optional[str] = None,
    ) -> ValidationRule:
        """Create a RANGE rule for numeric or temporal columns."""
        rule_name = name or f"range:{column}"
        return cls(
            name=rule_name,
            rule_type=RuleType.RANGE,
            column=column,
            params={
                "min_val": min_val,
                "max_val": max_val,
                "inclusive_min": inclusive_min,
                "inclusive_max": inclusive_max,
            },
        )

    @classmethod
    def regex(
        cls,
        column: str,
        pattern: str,
        name: Optional[str] = None,
    ) -> ValidationRule:
        """Create a REGEX pattern match rule."""
        rule_name = name or f"regex:{column}"
        return cls(
            name=rule_name,
            rule_type=RuleType.REGEX,
            column=column,
            params={"pattern": pattern},
        )

    @classmethod
    def format(
        cls,
        column: str,
        pattern: str,
        name: Optional[str] = None,
    ) -> ValidationRule:
        """Alias for regex format rule."""
        rule_name = name or f"format:{column}"
        return cls(
            name=rule_name,
            rule_type=RuleType.FORMAT,
            column=column,
            params={"pattern": pattern},
        )

    @classmethod
    def unique(
        cls,
        columns: Union[str, Sequence[str]],
        name: Optional[str] = None,
        keep: str = "first",
    ) -> ValidationRule:
        """Create a UNIQUE constraint rule over one or more columns."""
        cols = [columns] if isinstance(columns, str) else list(columns)
        rule_name = name or f"unique:{','.join(cols)}"
        return cls(
            name=rule_name,
            rule_type=RuleType.UNIQUE,
            columns=cols,
            params={"keep": keep},
        )

    @classmethod
    def allowed_values(
        cls,
        column: str,
        values: Sequence[Any],
        name: Optional[str] = None,
    ) -> ValidationRule:
        """Create an ALLOWED_VALUES categorical check rule."""
        rule_name = name or f"allowed_values:{column}"
        return cls(
            name=rule_name,
            rule_type=RuleType.ALLOWED_VALUES,
            column=column,
            params={"values": list(values)},
        )

    @classmethod
    def referential(
        cls,
        column: str,
        ref_df: Optional[DataFrame] = None,
        ref_column: Optional[str] = None,
        ref_values: Optional[Sequence[Any]] = None,
        name: Optional[str] = None,
    ) -> ValidationRule:
        """Create a REFERENTIAL integrity rule checking keys in a reference DataFrame or list."""
        rule_name = name or f"referential:{column}"
        return cls(
            name=rule_name,
            rule_type=RuleType.REFERENTIAL,
            column=column,
            params={
                "ref_df": ref_df,
                "ref_column": ref_column or column,
                "ref_values": list(ref_values) if ref_values is not None else None,
            },
        )

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> ValidationRule:
        """Instantiate a ValidationRule from a dictionary configuration."""
        d = dict(data)
        name = d.pop("name", None)
        rule_type = d.pop("type", d.pop("rule_type", None))
        if not rule_type:
            raise ValueError(f"Rule dictionary must specify 'type': {data}")

        column = d.pop("column", None)
        columns = d.pop("columns", None)

        rule_name = name or (
            f"{rule_type}:{column}"
            if column
            else f"{rule_type}:{','.join(columns or [])}"
        )
        return cls(
            name=rule_name,
            rule_type=rule_type,
            column=column,
            columns=columns,
            params=d,
        )


# Predefined source validation rules per RetailPulse specification
SALES_RULES: List[ValidationRule] = [
    ValidationRule.not_null("order_id", name="not_null:order_id"),
    ValidationRule.not_null("customer_id", name="not_null:customer_id"),
    ValidationRule.range(
        "quantity",
        min_val=1,
        inclusive_min=True,
        name="range:quantity_positive",
    ),
    ValidationRule.range(
        "unit_price",
        min_val=0.0,
        inclusive_min=True,
        name="range:unit_price_non_negative",
    ),
    ValidationRule.range(
        "order_timestamp",
        max_val="now",
        name="range:order_timestamp_not_future",
    ),
    ValidationRule.unique(
        ["order_id", "order_item_id"],
        name="unique:order_id,order_item_id",
    ),
]

INVENTORY_RULES: List[ValidationRule] = [
    ValidationRule.not_null("product_id", name="not_null:product_id"),
    ValidationRule.not_null("store_id", name="not_null:store_id"),
    ValidationRule.range(
        "stock_qty",
        min_val=0,
        inclusive_min=True,
        name="range:stock_qty_non_negative",
    ),
    ValidationRule.range(
        "snapshot_date",
        max_val="today",
        name="range:snapshot_date_not_future",
    ),
    ValidationRule.unique(
        ["product_id", "store_id", "snapshot_date"],
        name="unique:inventory_snapshot",
    ),
]

CUSTOMERS_RULES: List[ValidationRule] = [
    ValidationRule.not_null("customer_id", name="not_null:customer_id"),
    ValidationRule.not_null("first_name", name="not_null:first_name"),
    ValidationRule.not_null("city", name="not_null:city"),
    ValidationRule.regex(
        "email",
        pattern=r"^[\w\.-]+@[\w\.-]+\.\w+$",
        name="regex:valid_email",
    ),
    ValidationRule.unique(
        ["customer_id"],
        name="unique:customer_id",
    ),
]


def get_source_rules(source: str) -> List[ValidationRule]:
    """Return preconfigured validation rules for a known data source.

    Args:
        source: Name of source ('sales', 'inventory', 'customers').

    Returns:
        List of ValidationRule objects.

    Raises:
        ValueError: If source name is not recognized.
    """
    normalized = source.strip().lower()
    if normalized == "sales":
        return list(SALES_RULES)
    if normalized == "inventory":
        return list(INVENTORY_RULES)
    if normalized in ("customers", "customer"):
        return list(CUSTOMERS_RULES)
    raise ValueError(
        f"Unknown source '{source}'. Permitted sources: 'sales', 'inventory', 'customers'."
    )
