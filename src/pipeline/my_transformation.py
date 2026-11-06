from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.window import Window

from quarantine_rules import failed_rules_for

catalog = spark.conf.get("catalog")  # publish target (pipeline catalog field)
source_catalog = spark.conf.get("source_catalog", catalog)  # shared raw/bronze source (R6)

CHECKS = ["passes_q_a", "passes_q_b", "passes_q_c", "passes_q_d", "passes_q_e"]

RULE_CODES = {"passes_q_a": "Q-A", "passes_q_b": "Q-B", "passes_q_c": "Q-C",
              "passes_q_d": "Q-D", "passes_q_e": "Q-E"}

# Quarantine rules live in quarantine_rules.py (pure Python, importable and
# unit-testable without a cluster) and are applied to each deduped, typed
# claim row through this UDF.
rule_eval_udf = F.udf(failed_rules_for, ArrayType(StringType()))


@dp.materialized_view(name="claims_checked", private=True)
@dp.expect("passes_q_a", "passes_q_a")
@dp.expect("passes_q_b", "passes_q_b")
@dp.expect("passes_q_c", "passes_q_c")
@dp.expect("passes_q_d", "passes_q_d")
@dp.expect("passes_q_e", "passes_q_e")
def claims_checked():
    bronze = spark.read.table(f"{source_catalog}.bronze.claims")

    members = (spark.read.table(f"{source_catalog}.raw.members")
               .select(F.col("member_id").alias("valid_member")).distinct())
    hospitals = (spark.read.table(f"{source_catalog}.raw.hospitals")
                 .select(F.col("hospital_id").alias("valid_hospital")).distinct())

    # Dedupe FIRST: latest submitted_ts per claim_id
    w = Window.partitionBy("claim_id").orderBy(
        F.to_timestamp("submitted_ts").desc_nulls_last(),
        F.col("_ingest_ts").desc_nulls_last())
    deduped = (bronze.withColumn("_rn", F.row_number().over(w))
               .filter("_rn = 1").drop("_rn"))

    # Cast strings to real types
    cleaned = (deduped
        .withColumn("admission_date", F.expr("try_cast(admission_date AS DATE)"))
        .withColumn("discharge_date", F.expr("try_cast(discharge_date AS DATE)"))
        .withColumn("submitted_ts", F.expr("try_cast(submitted_ts AS TIMESTAMP)"))
        .withColumn("settled_ts", F.expr("try_cast(settled_ts AS TIMESTAMP)")))

    flagged = (cleaned
        .join(members, F.col("member_id") == F.col("valid_member"), "left")
        .join(hospitals, F.col("hospital_id") == F.col("valid_hospital"), "left"))

    # Quarantine rules (Q-A..Q-E): pure-Python logic from quarantine_rules.py,
    # applied per row. Semantics identical to the previous vectorized
    # expressions, including their null behaviour.
    flagged = (flagged
        .withColumn("member_ok", F.col("valid_member").isNotNull())
        .withColumn("hospital_ok", F.col("valid_hospital").isNotNull()))

    rule_input = F.struct(
        "member_ok", "hospital_ok",
        "admission_date", "discharge_date", "submitted_ts", "settled_ts",
        "status", "billed_amount", "approved_amount", "rejection_code",
        "room_days", "room_rent_per_day")

    flagged = flagged.withColumn("failed_rules", rule_eval_udf(rule_input))
    for check, code in RULE_CODES.items():
        flagged = flagged.withColumn(check, ~F.array_contains("failed_rules", code))

    return (flagged
        .drop("valid_member", "valid_hospital", "member_ok", "hospital_ok"))


@dp.materialized_view(name="claims_clean")
def claims_clean():
    return (spark.read.table("claims_checked")
        .filter(F.size("failed_rules") == 0)
        .drop("failed_rules", *CHECKS))


@dp.materialized_view(name="claims_quarantine")
def claims_quarantine():
    return (spark.read.table("claims_checked")
        .filter(F.size("failed_rules") > 0)
        .drop(*CHECKS))