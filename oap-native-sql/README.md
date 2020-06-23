# Spark Native SQL Engine

A Native Engine for Spark SQL with vectorze SIMD optimizations

## Introduction

![Overview](/oap-native-sql/resource/Native_SQL_Engine_Intro.jpg)

Spark SQL works very well with structured row-based data. Its vectorized reader and writer for parquet/orc can make I/O much faster. It also used WholeStageCodeGen to improve the performance by Java JIT code. However Java JIT is usually not working very well on utilizing latest SIMD instructions, espeically under complicated queries. Apache Arrow provides columnar in-memory layout and SIMD optimized kernels as well as a LLVM based SQL engine Gandiva. Native SQL Engine used these technoligies and brought better performance to Spark SQL.

## Key Features

### Apache Arrow formated intermediate data among Spark operator

![Overview](/oap-native-sql/resource/columnar.png)

With [Spark 27396](https://issues.apache.org/jira/browse/SPARK-27396) its possible to pass a RDD of Columnarbatch to operators. We implementd this API with Arrow columnar format.

### Apache Arrow based Native Readers for Paruqet and other formats

![Overview](/oap-native-sql/resource/dataset.png)

A native parquet reader was developed to speed up the data loading. it's based on Apache Arrow Dataset. For details please check [Arrow Data Source](../oap-data-source/README.md)

### Apache Arrow Compute/Gandiva based operators

![Overview](/oap-native-sql/resource/kernel.png)

We implemented common operatos based on Apache Arrow Compute and Gandiva. The SQL expression was compiled to one expression tree with protobuf and passed to native kernels. The native kernels will then evaluate the these expressions based on the input columnar batch.

### Native Columnar Shuffle Operator with efficient compression support

![Overview](/oap-native-sql/resource/shuffle.png)

We implemented columnar shuffle to improve the shuffle performance. With the columnar layout we could do very efficient data compression for different data format.

## Testing

Check out the detailed installation/testing guide(/oap-native-sql/resource/installation.md) for quick testing

## Contact

chendi.xue@intel.com
binwei.yang@intel.com
