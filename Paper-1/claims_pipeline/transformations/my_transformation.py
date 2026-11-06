from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

catalog = spark.conf.get("catalog")

CHECKS = ["passes_q_a", "passes_q_b", "passes_q_c", "passes_q_d", "passes_q_e"]

DATA_START_DATE = F.to_date(F.lit("2025-07-01"))
DATA_END_DATE = F.to_date(F.lit("2026-06-30"))


@dp.materialized_view(name="claims_checked", private=True)
@dp.expect("passes_q_a", "passes_q_a")
@dp.expect("passes_q_b", "passes_q_b")
@dp.expect("passes_q_c", "passes_q_c")
@dp.expect("passes_q_d", "passes_q_d")
@dp.expect("passes_q_e", "passes_q_e")
def claims_checked():
    bronze = spark.read.table(f"{catalog}.bronze.claims")

    members = (spark.read.table(f"{catalog}.raw.members")
               .select(F.col("member_id").alias("valid_member")).distinct())
    hospitals = (spark.read.table(f"{catalog}.raw.hospitals")
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

    # Q-A: referential integrity (members and hospitals only)
    flagged = flagged.withColumn("passes_q_a", F.coalesce(
        F.col("valid_member").isNotNull() & F.col("valid_hospital").isNotNull(),
        F.lit(False)))

    # Q-B: date ordering and data window
    valid_admission = (F.col("admission_date").isNotNull()
        & (F.col("admission_date") <= F.col("discharge_date"))
        & (F.col("admission_date") >= DATA_START_DATE)
        & (F.col("admission_date") <= DATA_END_DATE))
    valid_discharge = (F.col("discharge_date").isNotNull()
        & (F.col("discharge_date") >= DATA_START_DATE)
        & (F.col("discharge_date") <= DATA_END_DATE))
    valid_submit = (F.col("submitted_ts").isNotNull()
        & (F.to_date("submitted_ts") >= DATA_START_DATE)
        & (F.to_date("submitted_ts") <= DATA_END_DATE)
        & (F.to_date("submitted_ts") >= F.col("admission_date")))
    valid_settled = (F.col("settled_ts").isNull()
        | ((F.to_date("settled_ts") >= DATA_START_DATE)
           & (F.to_date("settled_ts") <= DATA_END_DATE)
           & (F.col("settled_ts") >= F.col("submitted_ts"))))
    flagged = flagged.withColumn("passes_q_b", F.coalesce(
        valid_admission & valid_discharge & valid_submit & valid_settled,
        F.lit(False)))

    # Q-C: amount sanity
    valid_billed = F.col("billed_amount").isNotNull() & (F.col("billed_amount") >= 0)
    valid_approved = (F.coalesce(F.col("approved_amount"), F.lit(0)) <= F.col("billed_amount")) \
        & (F.col("approved_amount").isNull() | (F.col("approved_amount") >= 0))
    flagged = flagged.withColumn("passes_q_c", F.coalesce(
        valid_billed & valid_approved, F.lit(False)))

    # Q-D: status consistency
    submitted_ok = (F.col("status") != "submitted") | (
        F.col("approved_amount").isNull() & F.col("rejection_code").isNull() & F.col("settled_ts").isNull())
    approved_ok = (F.col("status") != "approved") | (
        F.col("approved_amount").isNotNull() & F.col("rejection_code").isNull() & F.col("settled_ts").isNull())
    rejected_ok = (F.col("status") != "rejected") | (
        F.col("rejection_code").isNotNull() & F.col("settled_ts").isNull())
    settled_ok = (F.col("status") != "settled") | (
        F.col("approved_amount").isNotNull() & F.col("settled_ts").isNotNull() & F.col("rejection_code").isNull())
    valid_status = F.col("status").isin("submitted", "approved", "rejected", "settled")
    flagged = flagged.withColumn("passes_q_d", F.coalesce(
        valid_status & submitted_ok & approved_ok & rejected_ok & settled_ok,
        F.lit(False)))

    # Q-E: room math (same-day stay counts as 1 room-day)
    flagged = flagged.withColumn("passes_q_e", F.coalesce(
        F.col("room_days").isNotNull()
        & F.col("room_rent_per_day").isNotNull()
        & (F.col("room_days") == F.greatest(
            F.datediff("discharge_date", "admission_date"), F.lit(1)))
        & ((F.col("room_days") * F.col("room_rent_per_day")) <= F.col("billed_amount")),
        F.lit(False)))

    return (flagged
        .withColumn("failed_rules", F.array_compact(F.array(
            F.when(~F.col("passes_q_a"), F.lit("Q-A")),
            F.when(~F.col("passes_q_b"), F.lit("Q-B")),
            F.when(~F.col("passes_q_c"), F.lit("Q-C")),
            F.when(~F.col("passes_q_d"), F.lit("Q-D")),
            F.when(~F.col("passes_q_e"), F.lit("Q-E")))))
        .drop("valid_member", "valid_hospital"))


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