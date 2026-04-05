"""
ScyllaDB (Cassandra-compatible CQL) client setup.

Uses the official DataStax Python driver; Scylla speaks the same protocol.
"""
from __future__ import annotations

import os

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy


def contact_points() -> list[str]:
    raw = os.environ.get("SCYLLA_HOSTS") or os.environ.get("CASSANDRA_HOSTS") or "scylla-server"
    return [h.strip() for h in raw.split(",") if h.strip()]


def local_dc() -> str:
    # Matches default rack/DC in official Scylla Docker images (single-node).
    return os.environ.get("SCYLLA_LOCAL_DC", "datacenter1")


def make_cluster(*, connect_timeout: int = 15) -> Cluster:
    profile = ExecutionProfile(
        load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy(local_dc=local_dc())),
        consistency_level=ConsistencyLevel.LOCAL_ONE,
    )
    return Cluster(
        contact_points(),
        connect_timeout=connect_timeout,
        execution_profiles={EXEC_PROFILE_DEFAULT: profile},
    )
