## mumu-kite 大数据数据工具集

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/mumuhadoop/mumu-kite/blob/master/LICENSE)
[![Build Status](https://travis-ci.org/mumuhadoop/mumu-kite.svg?branch=master)](https://travis-ci.org/mumuhadoop/mumu-kite)
[![codecov](https://codecov.io/gh/mumuhadoop/mumu-kite/branch/master/graph/badge.svg)](https://codecov.io/gh/mumuhadoop/mumu-kite)

mumu-kite是一个demo项目，主要通过这个项目来了解kite到底是干什么的和怎么使用kite。kite是专门来操纵大数据集的，可以通过kite将数据存储到hdfs、local、hive、hbase中，并且还提供了partition分区机制，加快数据访问速度。并且kite支持avro、parquet、csv、json等几种存储数据的方式。

## kite dataset Scheme

Scheme | Pattern | Pattern2
---|--- | ---
Hive | dataset:hive:<namespace>/<dataset> | dataset:hive:/<path>/<namespace>/<dataset-name>
HDFS | dataset:hdfs:/<path>/<namespace>/<dataset-name> |
S3 | dataset:s3a://<bucket>/<namespace>/<dataset-name>|dataset:s3n://<bucket>/<path>/<namespace>/<dataset-name>
Local FS | dataset:file:/<path>/<namespace>/<dataset-name> |
HBase | dataset:hbase:<zookeeper>/<dataset-name> |


## Parquet vs Avro Format

Avro is a row-based storage format for Hadoop.

Parquet is a column-based storage format for Hadoop.

If your use case typically scans or retrieves all of the fields in a row in each query, Avro is usually the best choice.

If your dataset has many columns, and your use case typically involves working with a subset of those columns rather than entire records, Parquet is optimized for that kind of work.

## kite partition

You can partition your dataset on one or more attributes of an entity. Proper partitioning helps Hadoop store information for improved performance. You can partition your records using hash, identity, or date (year, month, day, hour) strategies.

## Kite Data Artifacts

You can use the Kite data API by adding dependencies for the artifacts described below.

- [kite-data-core](http://kitesdk.org/docs/1.1.0/dependencies/kite-data-core.html) has the Kite data API, including all of the Kite classes used in this introduction. It also includes the Dataset implementation for both HDFS and local file systems.

- [kite-data-hive](http://kitesdk.org/docs/1.1.0/dependencies/kite-data-hive.html) is a Dataset implementation that creates Datasets as Hive tables and stores metadata in the Hive MetaStore. Add a dependency on kite-data-hive if you want to interact with your data through Hive or Impala
 
- [kite-data-hbase](http://kitesdk.org/docs/1.1.0/dependencies/kite-data-hbase.html) is an experimental Dataset implementation that creates datasets as HBase tables.

- [kite-data-crunch](http://kitesdk.org/docs/1.1.0/dependencies/kite-data-crunch.html) provides helpers to use a Kite dataset as a source or target in a Crunch pipeline.

- [kite-data-mapreduce](http://kitesdk.org/docs/1.1.0/dependencies/kite-data-mapreduce.html) provides MR input and output formats that read from or write to Kite datasets.

## 相关阅读

[hadoop官网文档](http://hadoop.apache.org)

[kite：A Data API for Hadoop](http://kitesdk.org/docs/current/)

## 联系方式

以上观点纯属个人看法，如有不同，欢迎指正。

email:<babymm@aliyun.com>

github:[https://github.com/babymm](https://github.com/babymm)