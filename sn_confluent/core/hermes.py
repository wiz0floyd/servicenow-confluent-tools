"""Hermes (ServiceNow Kafka) AdminClient wrapper.

Provides KafkaAdminClient-backed helpers for topic listing and broker
topology discovery. Used by the deploy subcommand for both sink and source
connector configuration.

kafka-python is an optional runtime dependency — methods return None and
print a warning if it is not installed.
"""

from __future__ import annotations

import os
import shutil
import sys
import tempfile
from typing import List, Optional

from sn_confluent.core.pem import SN_SOURCE_CLUSTERS, SN_BROKERS_PER_CLUSTER

_SINK_BASE_PORT = 4000


class HermesClient:
    """Thin wrapper around kafka-python's KafkaAdminClient for a single Hermes instance."""

    def __init__(
        self,
        instance_name: str,
        ca_pem: bytes,
        client_cert_pem: bytes,
        client_key_pem: bytes,
        key_password: Optional[str] = None,
        brokers_per_cluster: int = SN_BROKERS_PER_CLUSTER,
    ) -> None:
        self.instance_name = instance_name
        self._brokers_per_cluster = brokers_per_cluster
        # kafka-python 2.x only accepts file paths (ssl_cafile etc.), not in-memory _data variants.
        self._tmpdir = tempfile.mkdtemp(prefix="hermes_ssl_")
        ca_path = os.path.join(self._tmpdir, "ca.pem")
        cert_path = os.path.join(self._tmpdir, "cert.pem")
        key_path = os.path.join(self._tmpdir, "key.pem")
        with open(ca_path, "wb") as f:
            f.write(ca_pem)
        with open(cert_path, "wb") as f:
            f.write(client_cert_pem)
        with open(key_path, "wb") as f:
            f.write(client_key_pem)
        self._ssl = dict(
            security_protocol="SSL",
            ssl_cafile=ca_path,
            ssl_certfile=cert_path,
            ssl_keyfile=key_path,
        )
        if key_password:
            self._ssl["ssl_password"] = key_password

    def __del__(self) -> None:
        shutil.rmtree(getattr(self, "_tmpdir", ""), ignore_errors=True)

    def _admin(self, bootstrap: str):
        try:
            from kafka.admin import KafkaAdminClient
        except ImportError:
            raise ImportError("kafka-python is required: pip install kafka-python") from None
        return KafkaAdminClient(bootstrap_servers=bootstrap, **self._ssl)

    @property
    def _hostname(self) -> str:
        """Return the broker hostname, appending .service-now.com if not already a FQDN."""
        if not self.instance_name:
            raise ValueError("instance_name must not be empty")
        if "." in self.instance_name:
            return self.instance_name
        return f"{self.instance_name}.service-now.com"

    def _bootstrap(self, base_port: int) -> str:
        return ",".join(
            f"{self._hostname}:{base_port + i}" for i in range(self._brokers_per_cluster)
        )

    def list_topics(self, base_port: int = _SINK_BASE_PORT) -> Optional[List[str]]:
        """Return sorted topic names from the cluster at base_port, or None on failure."""
        bootstrap = self._bootstrap(base_port)
        admin = None
        try:
            admin = self._admin(bootstrap)
            raw = admin.list_topics()
        except ImportError as exc:
            print(f"Warning: {exc}", file=sys.stderr)
            return None
        except Exception as exc:
            print(
                f"Warning: Could not connect to Hermes at {bootstrap}: {exc}",
                file=sys.stderr,
            )
            return None
        finally:
            if admin is not None:
                try:
                    admin.close()
                except Exception:
                    pass
        return sorted(
            t for t in raw
            if not t.startswith("__") and not t.startswith("_confluent")
        )

    def discover_broker_ports(self, base_port: int) -> Optional[List[int]]:
        """Connect to the cluster at base_port and return actual broker port numbers.

        Uses the Kafka metadata response — one bootstrap connection reveals the
        full broker list for that cluster.
        """
        bootstrap = f"{self._hostname}:{base_port}"
        admin = None
        try:
            admin = self._admin(bootstrap)
            future = admin._client.cluster.request_update()
            admin._client.poll(future=future, timeout_ms=10000)
            brokers = admin._client.cluster.brokers()
            if not brokers:
                print(
                    f"Warning: No brokers returned from {bootstrap}.",
                    file=sys.stderr,
                )
                return None
            return sorted(b.port for b in brokers)
        except ImportError as exc:
            print(f"Warning: {exc}", file=sys.stderr)
            return None
        except Exception as exc:
            print(
                f"Warning: Could not probe Hermes cluster at {bootstrap}: {exc}",
                file=sys.stderr,
            )
            return None
        finally:
            if admin is not None:
                try:
                    admin.close()
                except Exception:
                    pass

    def source_egress_endpoints(self) -> Optional[str]:
        """Probe both source peer clusters and return the confluent.custom.connection.endpoints value.

        Returns None on failure — caller should fall back to the default port ranges
        (SN_SOURCE_CLUSTERS / SN_BROKERS_PER_CLUSTER).
        """
        endpoints: List[str] = []
        for base_port in SN_SOURCE_CLUSTERS:
            ports = self.discover_broker_ports(base_port)
            if ports is None:
                return None
            print(
                f"  Source cluster {base_port}: {len(ports)} broker(s), "
                f"ports {ports[0]}-{ports[-1]}"
            )
            endpoints.extend(f"{self._hostname}:{p}" for p in ports)
        return ",".join(endpoints)


__all__ = ["HermesClient"]
