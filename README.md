Pgreplbot
=========

This is a draft project to build a health-checking service for postgresql replicas. The idea is to leverage haproxy +
this rest API health-checking service to automatically switch to healthy replicas, or fail to primary, when a replica
fails its health checks.

The criteria for a health replica is:
1. Must have an active replication slot.
2. Its byte-lag / time-lag must be under some threshold.

MIT
