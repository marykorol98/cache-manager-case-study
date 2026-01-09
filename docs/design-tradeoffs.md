# Design Trade-offs

## Parquet vs HDF5
Parquet offers better interoperability and columnar access, but requires
careful schema management.

## Dual Read Paths
Supporting both pandas and polars adds complexity, but enables efficient
partial access without breaking existing consumers.

## Safety Guards
Blocking persistence of incomplete data reduces flexibility but prevents
hard-to-debug cache corruption.
