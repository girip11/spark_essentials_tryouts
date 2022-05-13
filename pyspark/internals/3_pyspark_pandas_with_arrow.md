# PySpark Usage Guide for Pandas with Apache Arrow

- Apache arrow - in-memory columnar data format.
- This format is used to transfer data between python and JVM processes.
- We have to explicity configure for arrow usage in spark.
- pyarrow is prerequisite.
- Arrow optimization is made available when converting spark dataframe to pandas dataframe and vice versa.

```python
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
# incase of execution failure using arrow
spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
```

## Supported data types

Currently, all Spark SQL data types are supported by Arrow-based conversion except `MapType`, `ArrayType` of `TimestampType`, and nested `StructType`.

This is where enabling fallback will help incase of failures using arrow due to unsupported data types.

## Pandas udf

- `pandas_udf` decorator can be used.
- Columns of primitive types get passed as `Series` while `struct` columns get passed as `DataFrame`

```Python
@pandas_udf("col1 string, col2 long")
def func(s1: pd.Series, s2: pd.Series, s3: pd.DataFrame) -> pd.DataFrame:
    s3['col2'] = s1 + s2.str.len()
    return s3
```

Types of pandas UDFs.

- `pd.Series -> pd.Series`
- `Iterator[pd.Series] -> Iterator[pd.Series]` - In this case we get an iterator to a column(`pd.Series`)
- `Iterator[Tuple[pd.Series, ...]] -> Iterator[pandas.Series]` - Multiple columns in the input and output a single column.
- `pd.Series -> Scalar` - Computing `mean` for instance

## Map

- `pyspark.DataFrame.mapInPandas` maps an `Iterator[pd.DataFrame]` to `Iterator[pd.DataFrame]`. The best part is the returned dataframe can be of arbitrary length(rows may be added or dropped) compared to the original dataframe.

## Usage notes

- Spark data partitions are converted to arrow record batches. This could lead to high memory usage in the JVM.
- Batch size needs to be adjusted when the data is having too many columns.
- Setting `spark.sql.execution.arrow.maxRecordsPerBatch` is used for this. Default batch size is 10000.
- Thus based on this setting, single spark partition might translate to one or more arrow batches.

---

## References

- [PySpark Usage Guide for Pandas with Apache Arrow](https://spark.apache.org/docs/3.0.1/sql-pyspark-pandas-with-arrow.html)
