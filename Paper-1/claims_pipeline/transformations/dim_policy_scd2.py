from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.functions import expr

catalog = spark.conf.get("catalog")


@dp.temporary_view(name="policy_changes_typed")
def policy_changes_typed():
    """Typed CDC source: cast bronze string fields before Auto CDC processing."""
    return (spark.readStream.table(f"{catalog}.bronze.policy_changes")
        .select(
            "policy_id",
            "member_id",
            "plan_code",
            F.expr("try_cast(effective_from AS DATE)").alias("effective_from"),
            "op",
            F.expr("try_cast(op_ts AS TIMESTAMP)").alias("op_ts"))
        .filter(F.col("policy_id").isNotNull() & F.col("effective_from").isNotNull()))


dp.create_streaming_table(
    name="dim_policy_scd2",
    comment="SCD Type 2 policy dimension built from bronze.policy_changes CDC events")

dp.create_auto_cdc_flow(
    target="dim_policy_scd2",
    source="policy_changes_typed",
    keys=["policy_id"],
    sequence_by="effective_from",
    apply_as_deletes=expr("op = 'DELETE'"),
    stored_as_scd_type=2)
