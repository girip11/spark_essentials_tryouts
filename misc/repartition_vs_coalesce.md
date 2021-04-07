# Repartition vs Coalesce

## Repartition

- Can be used for increasing as well as decreasing partitions.
- This uses full shuffle and hence it is an expensive operation.
- Full shuffle - redistributes data from all the existing partitions and creates entirely new partitions of the specified count to store data.
- Creating new partitions helps to prevent skewed partitions and each partition will roughly be the same size.

### Possible issues with repartition

## Coalesce

- Used only for reducing the partitions.
- Data movement is minimized here. Hence it is considered an optimized version of repartition when decreasing the number of partitions.
- It tries to see if all partitions within the same executors could be merged to achieve reduction in partition numbers.
- Could result in partitions of **skewed sizes**.

### Coalesce operation shifting

- In a sequence of operations on a dataframe/RDD, coalesce could get shifted in 2 ways

  - Move right and sit next to the source/data load operation(example in the [Usecases](#usecases) section)
  - Propagate to the nearest shuffle.

## Usecases

### Writing to file

- When we want to collect all records and write to a single file, we might do something like `coalesce(1)` or `repartition(1)`

- When using `coalesce(1)` in some sequence like `load(...).map(...).filter(...).coalesce(1).save()`, the `coalesce` operation is moved to right just after `load`, (i.e) `load(...).coalesce(1).map(...).filter(...).save()`. Thus all loaded data is collected on to a single partition and subsequent operations run on it. Thus we lose parallelism here since map and filter will be operating on single partition.

- But `repartition()` is not moved before and using it will make `map(...).filter(...)` run in parallel on all the partitions and then repartition the results to 1 partition.

**NOTE**: Don't use coalesce for reducing the partition count to 1, in such cases use repartition. Using `coalesce(1)` could lead to [Out of memory error](https://stackoverflow.com/questions/38961251/java-lang-outofmemoryerror-unable-to-acquire-100-bytes-of-memory-got-0)

> However, if you're doing a drastic coalesce, e.g. to numPartitions = 1, this may result in your computation taking place on fewer nodes than you like (e.g. one node in the case of numPartitions = 1). To avoid this, you can call repartition(1) instead. This will add a shuffle step, but means the current upstream partitions will be executed in parallel (per whatever the current partitioning is). - Spark docs

### After filter operations

- After filter operation, if we have few records left in each partition, then we should do `repartition` so that subsequent operations run on smaller partitions rather than on large number of empty partitions.

### Repartitioning by column

- `someDataframe.repartition(col("col_name"))` - This will repartition the dataframe into 200 partitions by default. We have another overloaded method that we could use to control the partition count. `someDataframe.repartition(10, col("col_name"))` - creates 10 partitions based on the given column name.

- Use this technique on massive datasets, where repartition by a column or set of columns will improve the data distribution.

> `partitionBy` can be used to created data that’s partitioned on disk. `repartition` and `coalesce` partition data in memory. -[Managing Spark Partitions](https://mrpowers.medium.com/managing-spark-partitions-with-coalesce-and-repartition-4050c57ad5c4)

## Partitioning in memory

- We could estimate the number of records that sum up to 1GB and then repartition accordingly.

- Spark will execute `filter` operation differently based on the underlying datastore.

> - A parquet lake will send all the data to the Spark cluster, and perform the filtering operation on the Spark cluster
>
> - A Postgres database table will perform the filtering operation in Postgres, and then send the resulting data to the Spark cluster.
>
> - A Parquet data store will send the entire column to the cluster and perform the filtering on the cluster. This is referred to as **column pruning**(send only the columns required for processing).

### [Cluster size before and after filtering](https://mungingdata.com/apache-spark/filter-where/)

- Loading massive dataset from datastore like csv in to spark cluster is expensive and it would require a cluster with lots of resources. Once filtering is done on the cluster, then most of the resources remain idle. So the cluster might need to be scaled down otherwise we will pay a lot for the compute that was not used.
- Such scenarios going with the datastore that supports pushdown query would make the cluster size agnostic of the dataset size pre and post the filtering operation.

## [Partitioning on disk using `partitionBy`](https://mungingdata.com/apache-spark/partitionby/)

- `DataFrameWriter` contains `partitionBy` method. Enables writing data to the disk in nested folders.
- Here we can write the dataframe to the disk with the data partitioned based on column/columns. This would help us load only the required dataset thereby enabling us to filter the data right when reading from the disk itself.

---

## References

- [Repartition vs Coalesce](https://stackoverflow.com/questions/31610971/spark-repartition-vs-coalesce)

- [Spark Partitioning Strategies](https://medium.com/airbnb-engineering/on-spark-hive-and-small-files-an-in-depth-look-at-spark-partitioning-strategies-a9a364f908)
