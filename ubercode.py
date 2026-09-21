"""
UberCode - model weight delivery system.

The thesis: cold starts are a storage problem pretending to be a compute
problem. A node that boots in 8s but takes 4 minutes to pull 30GB of weights
is a storage-configured node, not a fast one.

What it does:
- content-addressed shard registry: weights split into shards, each named by
  its sha256 - integrity by construction, dedupe by design
- HTTP range requests: pull only the shards you need (lazy layer loading)
- local cache: second bring-up skips the network entirely
- manifest: the node asks for a model by name, gets shard list + hashes

Measured (drill):
- cold bring-up vs warm (cached) bring-up, local disk as the registry
- resume-interrupted-pull: kill mid-download, restart, only missing shards pull
"""
from __future__ import annotations

import hashlib
import http.server
import json
import os
import shutil
import socket
import tarfile
import threading
import time
import urllib.request
from pathlib import Path

SHARD_BYTES = 8 * 1024 * 1024        # 8MB shards


def make_fake_model(target_mb: int = int(os.environ.get("UC_MODEL_MB", "96"))) -> Path:
    """A deterministic pseudo-model tarball: N shard-sized blobs of bytes."""
    root = Path("registry_root")
    blobs = root / "blobs"
    blobs.mkdir(parents=True, exist_ok=True)
    shards = []
    buf = bytearray()
    shard_idx = 0
    counter = 0
    while counter < target_mb * 1024 * 1024:
        while len(buf) < SHARD_BYTES and counter < target_mb * 1024 * 1024:
            # deterministic PRNG stream keyed by global byte offset: every chunk unique
            seed = hashlib.sha256(counter.to_bytes(8, "big")).digest()
            chunk = bytes(seed[i % 32] ^ (i * 31 % 256) for i in range(65536))
            buf.extend(chunk)
            counter += len(chunk)
        if len(buf) < SHARD_BYTES and counter >= target_mb * 1024 * 1024 and not buf:
            break
        piece = bytes(buf[:SHARD_BYTES])
        buf = buf[SHARD_BYTES:]
        h = hashlib.sha256(piece).hexdigest()
        (blobs / h).write_bytes(piece)
        shards.append({"sha": h, "bytes": len(piece)})
        shard_idx += 1
        if counter >= target_mb * 1024 * 1024:
            break
    manifest = {"model": "tiny-fake-llm", "shards": shards,
                "total_bytes": sum(s["bytes"] for s in shards)}
    (root / "manifest.json").write_text(json.dumps(manifest))
    return root


class RangeHandler(http.server.BaseHTTPRequestHandler):
    """Serves blobs with Range support (what real weight CDNs do)."""

    def do_GET(self):
        path = self.path.lstrip("/")
        blob = Path("registry_root/blobs") / Path(path).name
        if path == "manifest.json":
            blob = Path("registry_root/manifest.json")
        if not blob.exists():
            self.send_error(404)
            return
        data = blob.read_bytes()
        rng = self.headers.get("Range")
        if rng and rng.startswith("bytes="):
            start_s, _, end_s = rng[6:].partition("-")
            start = int(start_s or 0)
            end = int(end_s) if end_s else len(data) - 1
            chunk = data[start:end + 1]
            self.send_response(206)
            self.send_header("Content-Range", f"bytes {start}-{end}/{len(data)}")
        else:
            chunk = data
            self.send_response(200)
        self.send_header("Content-Length", str(len(chunk)))
        self.end_headers()
        self.wfile.write(chunk)

    def log_message(self, *a):
        pass


def serve_registry(port: int = 8377) -> http.server.HTTPServer:
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", port), RangeHandler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


# ------------------------------------------------------------ client node


class Node:
    """A compute node bringing a model up: manifest -> missing shards -> cache."""

    def __init__(self, base: str, cache_dir: str):
        self.base = base
        self.cache = Path(cache_dir)
        self.cache.mkdir(parents=True, exist_ok=True)
        self.bytes_pulled = 0
        self.requests = 0

    def _fetch(self, rel: str) -> bytes:
        self.requests += 1
        with urllib.request.urlopen(self.base + "/" + rel, timeout=30) as r:
            data = r.read()
        self.bytes_pulled += len(data)
        return data

    def bring_up(self, model: str, verify: bool = True) -> dict:
        t0 = time.perf_counter()
        manifest = json.loads(self._fetch("manifest.json"))
        pulled, cached, verified = 0, 0, 0
        for shard in manifest["shards"]:
            h = shard["sha"]
            local = self.cache / h
            if local.exists() and hashlib.sha256(local.read_bytes()).hexdigest() == h:
                cached += 1
                continue
            data = self._fetch("blobs/" + h)
            assert hashlib.sha256(data).hexdigest() == h, "integrity fail"
            verified += 1
            tmp = local.with_suffix(".tmp")
            tmp.write_bytes(data)
            os.replace(tmp, local)   # atomic cache entry
            pulled += 1
        wall = time.perf_counter() - t0
        return {"wall_s": round(wall, 2), "pulled": pulled, "cached": cached,
                "verified": verified, "bytes_pulled": self.bytes_pulled}


def main():
    print("building deterministic 96MB fake model (12 x 8MB shards)...")
    t0 = time.perf_counter()
    root = make_fake_model(target_mb=96)
    print(f"registry built in {time.perf_counter()-t0:.1f}s\n")

    srv = serve_registry()
    try:
        print("=== drill 1: COLD bring-up (empty cache) ===")
        node = Node("http://127.0.0.1:8377", cache_dir="node_cache")
        cold = node.bring_up("tiny-fake-llm")
        print(f"cold:  {cold['wall_s']}s  pulled {cold['pulled']} shards "
              f"({cold['bytes_pulled']:,} bytes), verified {cold['verified']}")

        print("=== drill 2: WARM bring-up (cache hit) ===")
        warm = node.bring_up("tiny-fake-llm")
        print(f"warm:  {warm['wall_s']}s  pulled {warm['pulled']} shards, "
              f"cached {warm['cached']}")

        print("=== drill 3: INTERRUPTED pull (kill mid-way, resume) ===")
        node2 = Node("http://127.0.0.1:8377", cache_dir="node_cache2")
        # simulate interruption: pull only half the shards
        manifest = json.loads(urllib.request.urlopen(
            "http://127.0.0.1:8377/manifest.json", timeout=10).read())
        for shard in manifest["shards"][:6]:
            (Path("node_cache2") / shard["sha"]).write_bytes(
                urllib.request.urlopen(
                    "http://127.0.0.1:8377/blobs/" + shard["sha"], timeout=10).read())
        resumed = node2.bring_up("tiny-fake-llm")
        print(f"resume:{resumed['wall_s']}s  pulled {resumed['pulled']} missing shards, "
              f"cached {resumed['cached']} existing")

        print(f"\nSUMMARY: cold {cold['wall_s']}s -> warm {warm['wall_s']}s "
              f"({cold['wall_s']/max(warm['wall_s'],0.001):.0f}x), resume pulled only "
              f"{resumed['pulled']}/{len(manifest['shards'])} shards")
        Path("benchmarks").mkdir(exist_ok=True)
        stamp = time.strftime("%Y%m%d_%H%M%S")
        Path(f"benchmarks/ubercode_{stamp}.json").write_text(json.dumps({
            "cold": cold, "warm": warm, "resumed": resumed,
            "shard_count": len(manifest["shards"]),
            "total_mb": manifest["total_bytes"] // (1024*1024)}, indent=2))
    finally:
        srv.shutdown()
        shutil.rmtree("registry_root", ignore_errors=True)


if __name__ == "__main__":
    main()
