# Before → After (Cache Migration)

This comparison summarizes the shift from a legacy HDF5-based cache with per-direction logic to a metadata-driven Parquet cache that supports optional lazy reads. The goal was to keep the cache local and predictable while enabling partial access for large tabular datasets.

| Dimension | Before (Legacy Cache) | After (New CacheManager) | Why it matters |
|---|---|---|---|
| Storage format | HDF5 via custom storage handler | Parquet via pandas/Arrow with optional Polars reads | More interoperable columnar format |
| Read pattern | Mostly full materialization | Optional column/row subset reads | Supports preview-like access |
| Cache layout | Input/output split, per-direction logic | Unified metadata-driven layout | Fewer special cases in save/load |
| Compatibility hook | `__setstate__` fallback for handler | Explicit metadata store per node | Clearer reconstruction path |
| Failure modes | Handler exceptions | Runtime errors + warnings for unsafe states | More predictable operator feedback |