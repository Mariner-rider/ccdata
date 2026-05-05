# Data Pipeline (Optimized)

1. enqueue crawl task (redis/rq)
2. HTTP-first crawl with robots respect
3. WebClaw extract (or fallback)
4. normalize schema
5. store normalized record with freshness fields
6. trigger missing-field targeted crawl for official/trusted sources

## CollegeCue mapping
Crawler maps entities to CollegeCue normalized records with trust-tier and confidence score.

## Extraction quality rules
- Field-specific parsers use headings, lists, link text, regex and metadata.
- Every field has confidence and method in `field_details`.
- Missing-field checks require both empty/invalid value OR confidence below threshold.
- Use debug: `python -m services.lite_pipeline.main extract:debug --url file://tests/fixtures/college_sample.html`.

## Phase 7 quality gate and export
Records must pass completeness/confidence/trust/hash checks; failures go to quarantine_records.
Use source preview/dry-run and export command for safe production-style verification.

## Compliance logging and allowlist
Crawler logs robots allow/block, skipped binaries, allowlist blocks, and crawl errors in `crawl_logs`.
Pilot/preview/dry-run return quality_report for readiness checks.

## Phase 9 validation & operations
Use export validation to enforce clean outputs and readiness/audit commands for operational checks.

## Phase 10 real-site pilot lessons
- Prioritize Indian institutional URL patterns: programmes/program, academics, departments, fee-structure, admissions, placements/career-development, campus-life/hostel, faculty/people, contact/directory.
- Robots/compliance logs should be reviewed alongside missing fields before save.
- Use pilot dry-run iteratively to reduce missing fields before enabling save.

## Phase 11 lifecycle
Crawled records enter draft/needs_review and require approval before publish/chatbot sync.

## Phase 13
- Migration commands: `db:migrate`, `db:status`
- Write API key via `ADMIN_API_KEY`
- Publish/sync idempotency keys supported

## Queue and scheduler
Job lifecycle: queued -> running -> completed/failed/cancelled via `crawl_jobs` table. Memory queue for no-docker, Redis optional.

## Phase 15
Scheduler enforces due-date + budgets + cooldown and worker retries failed jobs with backoff; stale running jobs are recovered.
