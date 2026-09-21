"""Offline tests: UberCode integrity + cache logic, no network."""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ubercode import make_fake_model, Node  # noqa: E402


def test_deterministic_registry(tmp_path):
    """Same target bytes produce identical shard manifests (dedupe-ready)."""
    # build a small registry in a sandbox dir
    import os
    os.chdir(tmp_path)
    r1 = make_fake_model(target_mb=8)
    m1 = json.loads((r1 / "manifest.json").read_text())
    r2 = make_fake_model(target_mb=8)  # same content -> same hashes
    m2 = json.loads((r2 / "manifest.json").read_text())
    assert [s["sha"] for s in m1["shards"]] == [s["sha"] for s in m2["shards"]]


def test_manifest_integrity(tmp_path):
    import os
    os.chdir(tmp_path)
    root = make_fake_model(target_mb=8)
    m = json.loads((root / "manifest.json").read_text())
    assert sum(s["bytes"] for s in m["shards"]) == m["total_bytes"]
    for s in m["shards"]:
        blob = (root / "blobs" / s["sha"]).read_bytes()
        assert len(blob) == s["bytes"]


def test_range_semantics_dedupe(tmp_path):
    """Node re-pull of an already-cached shard must not hit the network twice."""
    import os
    os.chdir(tmp_path)
    root = make_fake_model(target_mb=8)
    from ubercode import serve_registry
    srv = serve_registry(port=8391)
    try:
        n1 = Node("http://127.0.0.1:8391", cache_dir="c1")
        a = n1.bring_up("m")
        b = n1.bring_up("m")
        assert b["pulled"] == 0 and b["cached"] == a["pulled"] + a["cached"]
    finally:
        srv.shutdown()
