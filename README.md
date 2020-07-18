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

### Future Work

1. PgReba should also be able to measure byte-lag from the primary server (not only the server ahead of it in the replication chain).

License MIT
