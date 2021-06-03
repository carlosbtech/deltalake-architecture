<p align="center">
  <a href="" rel="noopener">
 <img width=500px height=100px src="https://docs.delta.io/latest/_static/delta-lake-logo.png" alt="Project logo"></a>
</p>

<h3 align="center">Delta lake is an open-source project that enables building a Lakehouse architecture on top of existing storage systems such as S3, ADLS, GCS, and HDFS.</h3>

<div align="center">

[![Status](https://img.shields.io/badge/status-active-success.svg)]()
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](/LICENSE)

</div>

---

## 📝 Table of Contents

- [About](#about)
- [Architeture](#architeture)
- [Usage](#usage)
- [Built Using](#built_using)
- [Authors](#authors)

## 🧐 About <a name = "about"></a>

This project aims to make a simple etl processing, using pyspark with the deltalake framework. The work of pyspark will consume a filesystem titled landing-zone with files in json format. We will use some time travel techniques, writing in delta format for table management control and much more.

## 🔧 Architeture ELT Delta lake <a name = "architeture"></a>

![image](https://live-delta-io.pantheonsite.io/wp-content/uploads/2019/04/Delta-Lake-marketecture-0423c.png)

### Prerequisites

```
Spark: 3.1.1 https://spark.apache.org/downloads.html
```

## 🎈 Usage <a name="usage"></a>

## delta-bronze.py
The pyspark script - [delta-bronze.py] work reads data from a filesystem called landing-zone using deltalake dependencies, which are jar packages that are in spark's session config, with which it is possible to use the delta lake framework. after the execution of this script, the data will be written in the directory passed in code, inside the write table will be written a directory called _delta_log, which is responsible for storing incremental files on table metadata, it will be something like
00000000000000000000.json, 0000000000000000000001.json...
Json file under the _delta_log folder will have the information like add/remove parquet files(for Atomicity), stats(for optimized performance & data skipping), partitionBy(for partition pruning), readVersions(for time travel), commitInfo(for audit).

![image](https://miro.medium.com/max/1400/0*5XnRRdbrbuuNGFzJ.png)

## 🔧 Running the tests

```
pyspark < src/delta-bronze.py
```

### Results on delta/bronze

![img](https://github.com/carlosbpy/deltalake-architecture/blob/main/docs/img/Screen%20Shot%202021-06-03%20at%2013.18.08.png)

## delta-silver.py
In this step, the pyspark script - [delta-silver.py] reads the data in delta format, which results in a performance gain due to being stored in parquet format and having one of the great advantages of _delta_log metadata management, steps are performed processing in which unnecessary columns are removed and preparation of tables with join for MDW modeling with data normalized in dataset formats.
## 🔧 Running the tests

```
pyspark < src/delta-silver.py
```

### Results on delta/silver
![img](https://github.com/carlosbpy/deltalake-architecture/blob/main/docs/img/Screen%20Shot%202021-06-03%20at%2013.33.40.png)

## delta-gold.py
the spark consumer-zone job partitions the data by genre in order to seek better performance for the business analytical layer, metrics are also generated from how many male and female users are on the platform
## 🔧 Running the tests

```
pyspark < src/delta-gold.py
```

### Results of consumer-zone
![img](https://github.com/carlosbpy/pyspark-3.1.1-pgsql/blob/main/docs/consumer-zone.png)


## ⛏️ Built Using <a name = "built_using"></a>

- [Pyspark](https://spark.apache.org/docs/latest/api/python/index.html) - 3.1.1
- [Postgres](https://hub.docker.com/_/postgres) - Database

## ✍️ Authors <a name = "authors"></a>

- [@carlosbpy](https://github.com/carlosbpy) - Idea & Initial work