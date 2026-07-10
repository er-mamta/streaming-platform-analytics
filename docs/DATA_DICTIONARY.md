# Playback event data dictionary

The event is deliberately pseudonymous: `user_id` and `profile_id` are deterministic hashes in generated data, and no names, emails, or IP addresses are collected.

| Field | Type | Required | Description / rule |
|---|---|---:|---|
| `event_id` | string | yes | Globally unique idempotency key |
| `event_type` | enum | yes | `playback_started`, `playback_progress`, `playback_completed`, or `playback_error` |
| `event_ts` | timestamp | yes | UTC event time used for watermarks and windows |
| `ingestion_ts` | timestamp | yes | Client-side handoff time; compared with broker/processing time for lag |
| `user_id` | string | yes | Pseudonymous viewer identifier |
| `profile_id` | string | yes | Pseudonymous household profile identifier |
| `session_id` | string | yes | Playback session and broker partition key |
| `content_id` | string | yes | Stable content identifier |
| `content_title` | string | no | Human-readable demo label; a production model would join a content dimension |
| `device_type` | enum | yes | `connected_tv`, `mobile`, `tablet`, or `web` |
| `country` | string | yes | Two-letter market code |
| `subscription_tier` | enum | yes | `ad_supported`, `standard`, or `premium` |
| `position_seconds` | double | no | Playback-head position; must be non-negative |
| `watch_seconds` | double | no | Incremental watched duration carried by the event; must be non-negative |
| `startup_ms` | double | no | Time to first frame; normally populated on `playback_started` |
| `bitrate_kbps` | double | no | Selected playback bitrate; must be non-negative |
| `rebuffer_seconds` | double | no | Incremental stalled time; between zero and `watch_seconds` |
| `error_code` | string | no | Playback failure category; normally populated on `playback_error` |
| `schema_version` | integer | yes | Contract version; this release accepts version `1` |

## Gold metrics

| Metric | Definition |
|---|---|
| Plays | Count of `playback_started` events |
| Unique viewers | Exact distinct pseudonymous users locally; approximate distinct in Spark streaming |
| Watch hours | Sum of incremental `watch_seconds` divided by 3,600 |
| Completion rate | Completed events divided by plays, guarded for zero plays |
| Error rate | Playback errors divided by plays, guarded for zero plays |
| Rebuffer ratio | Rebuffer seconds divided by watch seconds |
| Average startup | Average non-null `startup_ms` |
| QoE score | `100 - startup penalty - rebuffer penalty - error penalty`, bounded to 0–100 |

The QoE score is intentionally explainable. It is a product hypothesis that should be calibrated against abandonment, support contacts, and experiment outcomes before business use.

