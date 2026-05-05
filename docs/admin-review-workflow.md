# Admin Review Workflow

States: `draft`, `needs_review`, `approved`, `rejected`, `published`.

## Direct record workflow
- `record:list --state draft`
- `record:show --id <id>`
- `record:approve --id <id> --reviewed-by admin` (draft/needs_review)
- `record:reject --id <id> --reviewed-by admin --notes "reason"`

## Review queue workflow
- `review:seed --entity-id <id>` to create queue entry for draft/needs_review
- `review:list`
- `review:approve --id <review_id> --reviewed-by admin`

## Publish + chatbot
- `publish:entity --id <entity_id>` (approved only)
- `chatbot:sync --entity-id <entity_id>` (published only)

## No-Docker lifecycle
1. source:add
2. source:crawl
3. record:list --state draft
4. record:approve
5. publish:entity
6. chatbot:sync
7. export:validate

Auto-publishing remains disabled by design.

## Phase 13 additions
- `db:migrate` / `db:status`
- direct API key protection on write endpoints (`X-API-Key`)
- idempotent publish/sync via idempotency key

## Monitoring review/publish ops
Use metrics and quality reports to track draft/review/published counts and missing-field trends.

## Public model separation
- Review/approval still acts on crawler records.
- Only published records are materialized into `public_entities` for public pages and search.
- This prevents raw extraction payloads from leaking to public APIs.

## Multi-category public flow
Approved records from all supported entity types are published into `public_entities` with category-specific `page_json` shapes.
