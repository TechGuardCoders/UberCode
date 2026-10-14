"""Fleet tests: per-node bring-up, failure isolation, node labels. No external network."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ubercode import Fleet, FLEET_DEFAULTS, make_fake_model, serve_registry  # noqa: E402


def test_fleet_bring_up_per_node(tmp_path, monkeypatch):
    import os
    monkeypatch.chdir(tmp_path)
    make_fake_model(target_mb=8)
    srv = serve_registry(port=8393)
    try:
        fleet = Fleet(nodes={"n1": "http://127.0.0.1:8393", "n2": "http://127.0.0.1:8393"},
                      cache_root="fleet_cache")
        out = fleet.bring_up("m")
        assert sorted(out["healthy"]) == ["n1", "n2"]
        assert out["failed"] == []
        for name, r in out["nodes"].items():
            assert r["node"] == name, "per-node provenance label"
        # second pass: both nodes warm, zero pulls
        again = fleet.bring_up("m")
        assert all(r["pulled"] == 0 for r in again["nodes"].values())
    finally:
        srv.shutdown()


def test_fleet_isolates_dead_node(tmp_path, monkeypatch):
    import os
    monkeypatch.chdir(tmp_path)
    make_fake_model(target_mb=8)
    srv = serve_registry(port=8394)
    try:
        fleet = Fleet(nodes={"live": "http://127.0.0.1:8394",
                             "dead": "http://127.0.0.1:1"},   # nothing listens
                      cache_root="fleet_cache2")
        out = fleet.bring_up("m")
        assert out["healthy"] == ["live"]
        assert len(out["failed"]) == 1 and out["failed"][0]["node"] == "dead"
    finally:
        srv.shutdown()


def test_fleet_defaults_are_labels_only():
    """Default fleet map carries the real node labels; no live calls in CI."""
    assert set(FLEET_DEFAULTS) == {"glm-head", "tensorfold", "bonsai-1", "bonsai-2", "sushi"}
