# Data Pipeline (Optimized)

1. enqueue crawl task (redis/rq)
2. HTTP-first crawl with robots respect
3. WebClaw extract (or fallback)
4. normalize schema
5. store normalized record with freshness fields
6. trigger missing-field targeted crawl for official/trusted sources
