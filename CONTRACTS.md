# CONTRACTS.md

## Kafka Topic
- Topic: `match_events`
- Partitions: 3
- Replication factor: 1
- Key: `match_id` (string)
- Value: JSON UTF-8

## Common Event Fields
Todos los eventos deben incluir:

```json
{
  "match_id": "123",
  "event_type": "GOAL",
  "event_id": "evt_g123",
  "timestamp": "2026-05-11T23:43:00Z"
}
```

## Event Types

### 1. KICKOFF
```json
{
  "match_id": "123",
  "event_type": "KICKOFF",
  "event_id": "evt_k001",
  "timestamp": "2026-05-11T23:43:00Z"
}
```

### 2. FOUL
```json
{
  "match_id": "123",
  "event_type": "FOUL",
  "event_id": "evt_f001",
  "team": "home",
  "timestamp": "2026-05-11T23:44:00Z"
}
```

### 3. GOAL
```json
{
  "match_id": "123",
  "event_type": "GOAL",
  "event_id": "evt_g123",
  "team": "home",
  "timestamp": "2026-05-11T23:45:00Z"
}
```

### 4. VAR_CHECK
```json
{
  "match_id": "123",
  "event_type": "VAR_CHECK",
  "event_id": "evt_v123",
  "team": "home",
  "related_event_id": "evt_g123",
  "timestamp": "2026-05-11T23:45:02Z"
}
```

### 5. GOAL_ANNULLED
```json
{
  "match_id": "123",
  "event_type": "GOAL_ANNULLED",
  "event_id": "evt_ga123",
  "annuls_event_id": "evt_g123",
  "timestamp": "2026-05-11T23:45:22Z"
}
```

### 6. MATCH_END
```json
{
  "match_id": "123",
  "event_type": "MATCH_END",
  "event_id": "evt_m001",
  "timestamp": "2026-05-11T23:50:00Z"
}
```

## Reglas de contrato
- Todos los mensajes se publican en `match_events`.
- La key Kafka siempre es `match_id`.
- `event_id` debe ser único.
- `GOAL_ANNULLED` debe referenciar el gol original mediante `annuls_event_id`.
- `team` solo aplica a eventos como `GOAL`, `FOUL`, `VAR_CHECK`.