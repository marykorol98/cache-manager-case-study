# Design Trade-offs

## Parquet vs HDF5
**Choice:** Persist cached tables as Parquet instead of HDF5.  
**Why:** The new cache writes data via `to_parquet`, enabling a columnar format with broad tooling support.  
**Risk:** Schema drift and mismatched column expectations when loading cached tables.  
**Mitigation:** Keep metadata-driven keys alongside the saved data and preserve original column names via explicit mappings during load.

## Dual Read Paths (pandas + Polars)
**Choice:** Support eager pandas reads and optional lazy/partial Polars reads.  
**Why:** Some consumers need full materialization, while preview-style access benefits from column/row slicing.  
**Risk:** Two code paths can diverge and increase maintenance burden.  
**Mitigation:** Keep a single entry point (`load_data_list`) and dispatch based on the `lazy_read` flag.

## Safety Guards for Incomplete Data
**Choice:** Avoid persisting incomplete data when the cache receives a `polars.DataFrame`.  
**Why:** Partial data is useful for previews but unsafe to store as authoritative cache entries.  
**Risk:** Users may expect preview data to persist across sessions.  
**Mitigation:** Emit warnings when incomplete data is passed to the cache and store only complete datasets.

## Metadata-Driven Layout vs Input/Output Split
**Choice:** Store a unified metadata list per node instead of separate input/output folders.  
**Why:** A single save/load pipeline reduces special-case logic and clarifies reconstruction.  
**Risk:** Migration requires carefully mapping legacy keys to the new structure.  
**Mitigation:** Preserve name/key transformations during load and keep compatibility logic near the data access layer.
