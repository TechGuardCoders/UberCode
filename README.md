# UberCode

**Model weight delivery — cold starts are a storage problem pretending to be a compute problem.**

A node that boots in 8s but spends 4 minutes pulling 30GB of weights is a storage-configured node, not a fast one. UberCode is the weight path: content-addressed shard registry, range-capable HTTP, integrity by construction, cache-aware bring-up.

Part of the [TechGuardCoders portfolio](https://github.com/orgs/TechGuardCoders/repositories): Cost Peep → Batcher → Spinal → Ball Knowledge → Bastion → Furnace → Truffle → Popcorn → QueuedGPU → Circuiter → **UberCode** → …

## Design

- **Content-addressed blobs** — every shard named by its sha256; integrity is verified on arrival, dedupe is free (identical shards collapse to one blob)
- **Manifest-driven bring-up** — node asks for a model by name, gets the shard list + hashes, pulls only what its cache is missing
- **Atomic cache writes** — tmp + rename; a killed pull never leaves a torn cache entry
- **Range-capable registry** — HTTP 206 semantics, the shape real weight CDNs speak

## Drill (measured, 96MB model = 12 × 8MB shards, localhost registry)

| Drill | Result |
|---|---|
| **Cold** bring-up (empty cache) | 1.34s — pulled 12 shards, 100,664,500 bytes, all verified |
| **Warm** bring-up (full cache) | 0.79s — **0 bytes over network** |
| **Interrupted** pull (6 of 12 pre-cached, simulating a killed pull) | 0.88s — **pulled only the 6 missing shards** |

The resume drill is the point: a node that died mid-download doesn't restart from zero — the manifest + content-addressing make "what's missing" computable, so only the gap gets pulled.

## Bugs the drills caught (the honest part)

1. **Determinism produced zero entropy** — the first shard-content formula `(counter*31 + i) % 256` generated *byte-identical* chunks every time (counter advanced 65536 per chunk; 31·65536 ≡ 0 mod 256). All 12 shards collapsed to 1 blob; "cold bring-up" pulled nothing because everything deduped. The registry was working perfectly on garbage data. Fix: hash-keyed PRNG stream keyed by global byte offset — deterministic AND unique per chunk.
2. **Dedupe as canary** — that bug surfaced *as* the dedupe feature: unique-sha count in the manifest was the tell. Content-addressing doesn't just dedupe; it *detects* when your generator is broken.

## Run it

```bash
uv venv --python 3.11 .venv && uv pip install pytest
./.venv/Scripts/python ubercode.py     # builds registry, serves, drills cold/warm/resume
./.venv/Scripts/python -m pytest tests/ -q
```

## What's next

Cold-start cost events feed Cost Peep (bring-up spend vs steady state); resume-from-partial pairs with Furnace's resume-from-checkpoint — storage and compute recovery as one story.
