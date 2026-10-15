"""Normalize recoverable formats before validation without hiding failed casts."""
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType


def from_bronze(spark: SparkSession, table) -> DataFrame:
    """Bronze retains text values; explicit schema also supports zero rows."""
    schema = StructType([StructField(c, StringType(), True) for c in table.column_names])
    return spark.createDataFrame(table.to_pylist(), schema)


def session(name: str) -> SparkSession:
    """Create the local laptop Spark runtime with predictable parsing behavior."""
    return (SparkSession.builder.master("local[2]").appName(name)
            .config("spark.sql.ansi.enabled", "false")
            .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "2").getOrCreate())


def prepare(df: DataFrame, source: str) -> DataFrame:
    """Normalize strings/dates; failed conversion becomes a rejected null."""
    df.sparkSession.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    df.sparkSession.conf.set("spark.sql.ansi.enabled", "false")
    for field in df.schema.fields:
        if field.dataType.simpleString() == "string":
            df = df.withColumn(field.name, F.trim(F.col(field.name)))
    for name in ("snapshot_date", "signup_date"):
        if name in df.columns:
            df = df.withColumn(name, F.coalesce(*[
                F.to_date(F.col(name), fmt) for fmt in ["yyyy-MM-dd", "dd/MM/yyyy", "MM/dd/yyyy"]]))
    if source == "customers":
        df = df.withColumn("email", F.lower(F.col("email")))
    return df
