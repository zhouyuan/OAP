# SparkColumnarPlugin

This project is to enable columnar processing operators for spark sql, columnar processing will use Apache Arrow to allocate memory and evaluate with LLVM. 

## Contents

- [Introduction](#introduction)
- [Installation](#installation)
- [Benchmark](#benchmark)
- [Contact](#contact)

## Introduction

#### Two key concepts of this project:
1. Use arrow as column vector format as intermediate data among spark operator.
2. Use Gandiva to evaluate columnar operator expressions.

![Overview](/resource/overview.jpg)

## Installation

#### Build and install 1.11.0 Parquet, we will leverage Parquet-Arrow 1.11.0 version for convering Parquet Schema to Arrow Schema in spark. So make sure, Parquet-Arrow 1.11.0 installed in your mvn dependency path.

``` shell
wget http://archive.apache.org/dist/thrift/0.12.0/thrift-0.12.0.tar.gz
tar xzf thrift-0.12.0.tar.gz
cd thrift-0.12.0
chmod +x ./configure
./configure --disable-gen-erl --disable-gen-hs --without-ruby --without-haskell --without-erlang --without-php --without-nodejs
make install

yum install boost-devel
./configure --disable-gen-erl --disable-gen-hs --without-ruby --without-haskell --without-erlang --without-php --without-nodejs
make install -j

git clone https://github.com/apache/parquet-mr.git
cd parquet-mr
git checkout apache-parquet-1.11.0
mvn clean install -pl parquet-arrow -am -DskipTests
ls /root/.m2/repository/org/apache/parquet/parquet-arrow/1.11.0/
parquet-arrow-1.11.0.jar  parquet-arrow-1.11.0.jar.lastUpdated  parquet-arrow-1.11.0.pom  parquet-arrow-1.11.0.pom.lastUpdated  parquet-arrow-1.11.0-tests.jar  _remote.repositories
```

#### Install Apache Arrow and Gandiva

Please refer this markdown to install Apache Arrow and Gandiva.
[Apache Arrow Installation](/resource/ApacheArrowInstallation.md)

#### Build and install a columnarSupported spark

``` shell
git clone https://github.com/intel-bigdata/sparkv.git
cd sparkv
./build/mvn -Pyarn -Phadoop-3.2 -Dhadoop.version=3.2.0 -DskipTests clean install
```

#### Install Googletest and Googlemock

``` shell
git clone https://github.com/google/googletest.git
cd googletest
mkdir build
cd build
cmake -DBUILD_GTEST=ON -DBUILD_GMOCK=ON -DINSTALL_GTEST=ON -DINSTALL_GMOCK=ON ..
make -j
make install
```

#### Build this project

This project has dependencies of Apache Spark and Apache Arrow, so please make sure these two project has been installed under /root/.m2/repository/ before start build this project.
``` shell
cd SparkColumnarPlugin
# libspark_columnar_jni.so is used for sparkColumnarPlugin java codes to call native functions.
cd cpp/
mkdir build/
cd build/
cmake ..
#cmake .. -DTESTS=ON
make -j
# copy all the missing dependencies from arrow/cpp/src/ to /usr/local/include/
make install
cd ../../core/
mvn package
```

#### spark configuration

``` shell
cat spark-defaults.xml

spark.driver.extraClassPath /mnt/nvme2/chendi/SparkColumnarPlugin/core/target/core-1.0-jar-with-dependencies.jar:/mnt/nvme2/chendi/arrow/java/gandiva/target/arrow-gandiva-0.14.0.jar
spark.executor.extraClassPath /mnt/nvme2/chendi/SparkColumnarPlugin/core/target/core-1.0-jar-with-dependencies.jar:/mnt/nvme2/chendi/arrow/java/gandiva/target/arrow-gandiva-0.14.0.jar

org.apache.spark.example.columnar.enabled true
spark.sql.extensions com.intel.sparkColumnarPlugin.ColumnarPlugin
spark.sql.columnVector.arrow.enabled true
```

## Benchmark

For initial microbenchmark performance, we add 10 fields up with spark, data size is 200G data

![Performance](/resource/performance.png)

## Coding Style

* For Java code, we used [google-java-format](https://github.com/google/google-java-format)
* For Scala code, we used [Spark Scala Format](https://github.com/apache/spark/blob/master/dev/.scalafmt.conf), please use [scalafmt](https://github.com/scalameta/scalafmt) or run ./scalafmt for scala codes format
* For Cpp codes, we used Clang-Format, check on this link [google-vim-codefmt](https://github.com/google/vim-codefmt) for details.

## contact

chendi.xue@intel.com
jian.zhang@intel.com
