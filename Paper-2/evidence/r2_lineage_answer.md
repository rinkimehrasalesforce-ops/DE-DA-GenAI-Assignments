# R2 — Lineage evidence: tables reading `claims.raw.members.phone`

Full three-part names of all tables that read (are downstream of) the `phone` column of `claims.raw.members`:

1. `claims.gov.proof_members_admin` — consumes it as `claims.gov.proof_members_admin.phone` (unmasked PII copy, admin view)
2. `claims.gov.proof_members_masked` — consumes it as `claims.gov.proof_members_masked.phone` (masked view: `****` + last 4 digits)

## Method

- Unity Catalog column-level lineage: `system.access.column_lineage` filtered on
  `source_table_full_name = 'claims.raw.members'` and `source_column_name = 'phone'`.
- Cross-checked with the Unity Catalog lineage API (column-level downstream references of `claims.raw.members.phone`).

Both methods return exactly the same two tables. No `claims.silver` or `claims.gold` table consumes
`members.phone` — the claims pipelines and gold joins use `member_id`, `policy_id` and `hospital_id`
as keys; the member PII columns are only read by the R1 proof tables in `claims.gov`.

Generated: 2026-11-05 (Paper 2, R2).
