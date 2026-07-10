# Implementation milestones

This plan turns the portfolio starter into a production-grade streaming data product without hiding risk behind a single “build the pipeline” milestone.

| Milestone | Scope | Definition of done | Interview signal |
|---|---|---|---|
| 1. Event contract and replayable data | Define playback events, generate deterministic data, hash user identifiers, include invalid and duplicate examples | Contract is versioned; fixtures are deterministic; unit tests cover required fields and ranges | Data contracts, privacy, testability |
| 2. Durable ingestion | Publish events with session keys; land payload plus broker metadata in Bronze; checkpoint offsets | Restarting does not lose acknowledged events; offsets and raw payload support replay | Kafka semantics, idempotency, lineage |
| 3. Trusted Silver layer | Parse with an explicit schema; quarantine invalid records; apply a 15-minute watermark; deduplicate by event ID | Duplicate and malformed inputs never reach consumer tables; late-data policy is documented | Spark state, watermarks, DQ gates |
| 4. Business-ready Gold marts | Build hourly and daily engagement, funnel, reliability, and QoE measures | SQL and the dashboard calculate the same documented metrics; zero denominators are safe | Dimensional thinking, KPI ownership |
| 5. Operations and observability | Add quality thresholds, freshness/lag metrics, checkpoint monitoring, retry policy, and an Airflow control-plane DAG | Alerts are actionable and have owners; a replay runbook exists; stream restarts are tested | SLA ownership, incident response |
| 6. Cloud deployment and scale test | Parameterize environments; deploy to GCP or Databricks; partition/cluster tables; load test skew and late arrivals | Infrastructure is reproducible; p95 latency and cost per million events are measured | Cloud architecture, performance, FinOps |

## Suggested build sequence

### Portfolio release — included here

- A deterministic 250-event dataset plus contract edge cases.
- A zero-dependency local Medallion pipeline.
- Kafka-compatible producer and three Spark Structured Streaming jobs.
- Delta upserts for mutable hourly windows.
- BigQuery analysis examples, a Streamlit dashboard, tests, and CI.

### Cloud release — next iteration

- Land Bronze and Silver on GCS/Delta or managed Databricks storage.
- Publish serving tables to BigQuery or Databricks SQL.
- Add Cloud Composer for quality checks, compaction, and replay workflows.
- Export Kafka/Spark metrics to Cloud Monitoring or Prometheus/Grafana.
- Add Terraform modules and separate dev/stage/prod configuration.

### Scale and reliability release

- Generate at least 10 million events with hot-content and hot-country skew.
- Measure event-time lag, processing latency, state-store size, and cost.
- Test broker interruption, executor loss, corrupt payloads, and checkpoint recovery.
- Compare partition strategies and document the chosen tradeoff with evidence.

## Production SLO candidates

These are design targets, not measured claims:

- 99% of valid playback events visible in Gold within 5 minutes.
- No acknowledged event loss during a single broker or executor failure.
- Duplicate rate below 0.1% after Silver deduplication.
- Daily quality pass rate at or above 98%; invalid records remain replayable.
- Gold metric reconciliation difference below 0.01% against source event counts.

