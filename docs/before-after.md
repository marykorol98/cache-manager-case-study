# Before → After (Cache Migration)

| Dimension | Before (Legacy Cache) | After (New CacheManager) | Why it matters |
|---|---|---|---|
| Storage format | HDF5 via custom storage handler | Parquet via pandas/Arrow + optional Polars | Interoperability + columnar access |
| Read pattern | Mostly full materialization | Optional column/row subset reads (Polars) | Lower memory for previews |
| Cache layout | Input/output split, per-direction logic | Unified metadata-driven layout | Simpler, fewer special cases |
| Compatibility | `__setstate__` fallback for handler | Backward-friendly behavior retained | Safer migration |
| Failure modes | Handler exceptions | Clear errors + warnings for unsafe states | Predictability |