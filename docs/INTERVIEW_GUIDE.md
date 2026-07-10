# Interview guide

## 60-second project story

“I built StreamPulse to model how a streaming platform can turn noisy playback telemetry into trusted engagement and quality metrics. Events are partitioned by session and ingested into an immutable Bronze Delta table. Spark parses them with an explicit contract, quarantines invalid payloads, applies an event-time watermark, and deduplicates by event ID in Silver. A stateful Gold job computes hourly playback starts, watch time, completion, errors, rebuffering, and an explainable QoE score, then uses idempotent Delta merges because late events can revise an open window. I also built a lightweight local path, SQL analysis, a dashboard, tests, CI, and an Airflow quality gate. The main production tradeoffs are event-time correctness versus state cost, approximate distinct counts versus latency, and replayability versus storage cost.”

## Design decisions worth discussing

- **Session ID is the broker key.** It preserves per-session ordering while distributing different sessions across partitions. A celebrity premiere can still create content-level skew downstream, so aggregations should salt or repartition hot keys when scale tests show pressure.
- **Bronze is append-only.** Raw payload and broker coordinates make reprocessing and audit possible. Retention and encryption policies contain the storage and privacy cost.
- **The watermark is 15 minutes.** It bounds state and accepts a documented late-data tradeoff. A real value should come from observed lateness percentiles, not intuition.
- **Silver dedupes on event ID.** Producer retries are expected under at-least-once delivery. Checkpoints plus idempotent Gold merges keep consumers stable.
- **Gold uses `foreachBatch` and Delta `MERGE`.** Streaming windows can update as valid late events arrive; simple append output would expose partial duplicates.
- **Airflow is a control plane.** It schedules bounded quality, compaction, reconciliation, and backfill tasks; it does not orchestrate each streaming event.

## Failure scenarios

| Scenario | Expected behavior |
|---|---|
| Producer retries an acknowledged message | Duplicate lands in Bronze and is removed in Silver by `event_id` |
| Payload violates contract | Raw record remains in Bronze; enriched copy lands in quarantine; Gold is unaffected |
| Event arrives within watermark | Its event-time window is updated and merged into Gold |
| Event arrives after watermark | It follows the documented late-data path; production can replay it in a correction job |
| Spark executor restarts | Query restores offsets and state from the checkpoint |
| Gold write retries | Merge keys and source batch metadata make the update idempotent |

## Honest scope statement

The repository includes production-shaped code and a fully runnable local mini-batch path. It does not claim measured throughput, latency, availability, or cloud cost. Those become credible only after the scale-and-reliability milestone records real benchmarks.

