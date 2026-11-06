# DE-Paper-1 — Claims Lakehouse Runbook

Medallion claims lakehouse on Databricks: bronze ingestion from files landing in a
UC Volume, silver quality gating + SCD2 policy dimension in a Lakeflow Spark
Declarative Pipeline, gold enrichment + reporting tables, governance (masking,
row filters, audit), a file-arrival-triggered Lakeflow Job, local unit tests for
the quarantine rules, and this runbook.

Repo layout:

    databricks.yml                  bundle: targets dev (claims_dev) / prod (claims_prod)
    resources/pipelines/            pipeline resource (libraries, configs)
    resources/jobs/                 job resource: ingest -> pipeline -> dq_check -> dq_gate -> publish_gold
    src/pipeline/                   pipeline source deployed by the bundle
      my_transformation.py          claims_checked / claims_clean / claims_quarantine (imports quarantine_rules)
      quarantine_rules.py           pure-Python quarantine rules Q-A..Q-E (importable, unit-testable)
      dim_policy_scd2.py            Auto CDC SCD2 policy dimension
    tests/                          pytest suite for the quarantine rules (no cluster needed)
    Paper-1/                        ingestion + gold notebooks, standalone claims_pipeline copy
    Paper-2/                        governance/audit notebooks, DQ check, evidence/

Catalogs: `claims` holds raw/bronze/silver/gold (source of truth); `claims_dev` and
`claims_prod` are where the bundle targets publish their silver/gold outputs. The
bundle pipeline always reads bronze from `claims` (the `source_catalog` conf) and
publishes into its own catalog (the `catalog` conf) — see `databricks.yml` variables.

## CLI setup

How to run:

1. Install the Databricks CLI (v1.24.0 was used; on the workspace driver — an
   aarch64 host — use the `linux_arm64` build, the x86_64 binary fails with
   `Exec format error`):

       curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
       databricks version

2. Authenticate with a personal access token (create one under User Settings →
   Developer → Access tokens) and store it in a profile OUTSIDE the Git folder:

       export DATABRICKS_HOST=https://<workspace-host>
       export DATABRICKS_TOKEN=<PAT>
       # or persist it:
       databricks configure --profile paper1

   Keep `~/.databrickscfg` private (chmod 600) and never commit the token.

How to confirm:

    databricks -p paper1 catalogs list

returns the workspace catalogs including `claims` and `claims_dev`. Prior CLI
session evidence: `Paper-2/evidence/cli/r5_cli_log.txt`.

## Bundle deploy

How to run (from the repo root, where `databricks.yml` lives):

    databricks bundle validate --target dev     # check config resolves
    databricks bundle deploy --target dev       # deploy pipeline + job
    databricks bundle run claims_etl_job --target dev   # full run: ingest -> pipeline -> dq gate -> gold

Variables (databricks.yml): `source_catalog` (default `claims`) and
`target_catalog` (`claims_dev` for dev, `claims_prod` for prod). The dev target
also adds the `[dev <user>]` name prefix and development-mode settings.

How to confirm:

- `databricks bundle summary --target dev` lists the deployed pipeline
  `[dev <user>] claims-etl-pipeline-dev` and job `[dev <user>] claims-etl-job-dev`.
- The job run goes green end-to-end (all 5 tasks SUCCESS); the DQ gate task
  (`dq_check` -> `dq_gate`) fails the run if critical nulls appear in silver.
- Output tables exist and are fresh: `claims_dev.silver.claims_clean`,
  `claims_dev.silver.claims_quarantine`, `claims_dev.silver.dim_policy_scd2`.
- Gotcha: if you change pipeline settings, a *retry* of a failed update reuses
  the old request's config snapshot — stop the pipeline and start a fresh update.

## Run the tests locally

The quarantine rules live in `src/pipeline/quarantine_rules.py` as pure-Python
functions — importing them needs no cluster, no Spark session, no pipeline run.

How to run (any laptop with Python 3.10+ and pytest):

    python -m pytest tests -v

Inside the Databricks workspace filesystem (/Workspace), bytecode caching is not
supported, so run:

    PYTHONDONTWRITEBYTECODE=1 python -m pytest tests -v -p no:cacheprovider

How to confirm:

- Summary reads `15 passed, 0 failed, 0 errors` in well under a second
  (measured: 0.19s). The suite covers one clean row, one failing row per rule
  Q-A..Q-E, the dedupe-by-submission-time tie-break, and rule boundaries
  (data window, same-day room-day floor, null handling).
- Recorded evidence: `Paper-2/evidence/r7_pytest_log.txt` (states the backend
  that ran the tests).
- The pipeline imports the very same module (see `my_transformation.py`), so a
  green suite means the deployed quarantine behaviour is what you just tested.

## Per-requirement quick reference (how to run / how to confirm)

### R1 — Governance
Run: `Paper-2/00_R1` notebook (admin group, `mask_last4` column mask on
raw.members PII, state row filter + grants, proof tables; some cells need to be
run by a user with grant permissions).
Confirm: `SHOW CREATE TABLE claims.raw.members` shows the mask on
national_id/phone; proof tables under `claims.gov.*` are populated.

### R2 — Lineage
Run: documented analysis in `Paper-2/evidence/r2_lineage_answer.md`.
Confirm: file lists the tables reading raw.members.phone
(claims.gov.proof_members_admin, claims.gov.proof_members_masked), cross-checked
against system.access.column_lineage and the lineage API.

### R3 — Silver quality + audit
Run: `databricks bundle run claims_etl_job --target dev` (or the standalone
`claims_pipeline` under Paper-1/ for the claims catalog).
Confirm: quarantine splits match the five expectations (failures visible in the
pipeline's DQ metrics); audit trail table `claims.gov.audit_claims_enriched`
(built in `Paper-2/00_R1`) has its rows with changed_at/changed_by/operation.

### R4 — Orchestrated job with trigger
Run: `claims_daily` job (id 519362464251275) has a file-arrival trigger on
/Volumes/claims/raw/landing/claims_raw/ (60s min interval); or run manually via
`databricks bundle run claims_etl_job --target dev`.
Confirm: dropping a new batch file into the volume fires a run whose trigger is
FILE_ARRIVAL and all tasks succeed; bronze row count grows by exactly the new
batch size (Auto Loader checkpoint keeps re-runs idempotent).

### R5 — CLI
Run: the CLI session recorded in `Paper-2/evidence/cli/r5_cli_log.txt`
(catalogs list, volume file ops via `databricks fs`, `jobs run-now`).
Confirm: log shows each command and its output, including a successful
end-to-end job run.

### R6 — Visual no-code rebuild
Run: open the `hospital_monthly_designer` visual data prep (Paper-1) and run its
flow; it overwrites `claims.gold.hospital_monthly_designer`.
Confirm: `claims.gold.hospital_monthly_designer` row-for-row equals the
code-built `claims.gold.hospital_monthly` (468 rows; EXCEPT in both directions
returns 0).

### R7 — Local development
Run: `python -m pytest tests -v` (see "Run the tests locally" above).
Confirm: 15 passed / 0 failed / 0 errors; evidence in
`Paper-2/evidence/r7_pytest_log.txt`; rules module imported by the deployed
pipeline (src/pipeline/my_transformation.py).

### R8 — Runbook
Run: this README.
Confirm: a stranger can set up the CLI, deploy the bundle, and run the tests
using only the sections above.

## Submission

Build in the private GitHub repo created from TEMPLATE_REPO and linked to the
workspace as a Git folder (this repo). When finished: run the grader per the
Assignment Notes, commit `evidence/scorecard.json` with the evidence files, and
tag the final commit `submission`.
