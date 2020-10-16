PgReba
======

Replica balancer and postgres health check service. A building-block in your postgres infrastructure.

PgReba is very similar in shape to the patroni health-checking API.

Supports postgres >= 10.2

### API

#### `GET /` or `GET /primary`

The endpoint will return a 200 when the postgres server is a primary. Otherwise, 503.

#### `GET /replica`

The endpoint will return a 200 when the postgres server is a replica. Otherwise, 503.

You can optionally pass a `max_allowable_byte_lag` query param to the `/replica` endpoint. This will connect to the upstream
database to measure true byte-lag from the primary. This is currently limited to 1 hop.

---

License MIT
