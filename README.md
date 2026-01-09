# Node Cache Manager — Production Migration Case Study (Anonymized)

## Executive Summary
I migrated a legacy cache implementation used for graph nodes to a new cache manager based on Parquet.
The new design enables partial and lazy reads for large datasets, improves reliability, and simplifies storage while preserving backward compatibility.
The cache is used to store intermediate data produced by nodes in a computational graph and is shared across services.

---

## Overview
This repository presents an anonymized case study of a **production cache migration** for node-level data in a graph-based system.

Each node produces tabular data that must be cached locally for reuse, inspection, and downstream processing.
The original cache implementation accumulated complexity and performance limitations over time, motivating a redesign rather than incremental patching.

This case study focuses on **engineering decisions**, migration strategy, and trade-offs, not on proprietary business logic.

---

## Project Context
- **Domain:** Caching intermediate data for graph nodes (pipelines / DAG-like execution)
- **Cache location:** Local filesystem
- **Data shape:** Tabular data (pandas / Arrow-compatible)
- **Usage patterns:** Full materialization and partial previews
- **Constraints:** Backward compatibility, reliability, and safe failure modes

---

## My Role
- Designed and implemented a new cache manager to replace a legacy implementation
- Migrated storage format from HDF5 to Parquet
- Introduced lazy and partial reads using Polars
- Preserved compatibility with existing cached data
- Added guards and validation to prevent unsafe caching behavior

---

## Problem Statement
The legacy cache implementation had several limitations:
- Custom storage handler with HDF5-based persistence
- Full materialization required for most reads
- Limited support for partial reads or column projection
- Increasing complexity around input/output separation
- Harder interoperability with modern data tooling

These issues became more pronounced as dataset sizes and usage scenarios grew.

---

## Solution Overview
I introduced a new `CacheManager` with the following characteristics:
- **Parquet-based storage** for cached data
- Unified metadata-driven save/load logic
- Support for eager (pandas) and lazy/partial (polars) reads
- Explicit handling of remote references (not cached locally)
- Safety checks to avoid persisting incomplete data

The new design simplifies storage while enabling more efficient data access patterns.

---

## Key Design Decisions
- **Parquet as the storage format:** columnar, efficient, interoperable
- **Dual read paths:** pandas for full reads, polars for partial/lazy access
- **Metadata-driven cache layout:** reduces special-case logic
- **Backward compatibility:** legacy data remains readable
- **Fail-safe behavior:** clear errors and warnings instead of silent corruption

---

## Representative Before → After

See `docs/before-after.md` for a detailed comparison.

Highlights:
- Storage: `.h5` + custom handler → `.parquet` via pandas/polars
- Reads: full load only → column- and row-level reads
- Structure: input/output split → unified node cache metadata

---

## Outcomes
- Enabled partial data access for large cached tables
- Reduced unnecessary memory usage during previews
- Simplified cache layout and persistence logic
- Improved maintainability and extensibility of the cache layer

---

## What Is Intentionally Omitted
- Company and product names
- Internal service boundaries
- Exact performance metrics
- Production infrastructure details

This repository focuses on architectural and engineering reasoning.

---

## Topics for Further Discussion (Optional)
- Cache design for graph-based execution systems
- Storage format trade-offs (HDF5 vs Parquet)
- Lazy vs eager data access patterns
- Backward compatibility strategies during migrations
