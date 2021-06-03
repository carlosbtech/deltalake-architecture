<p align="center">
  <a href="" rel="noopener">
 <img width=400px height=200px src="https://adatis.co.uk/wp-content/uploads/DeltaLakelogo1000x590.jpg" alt="Project logo"></a>
</p>

<h3 align="center">ETL using delta lake as framework.</h3>

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
The pyspark script - [delta-bronze.py] work reads data from a filesystem called landing-zone using deltalake dependencies, which are jar packages that are in spark's session config, with which it is possible to use the delta lake framework.
## 🔧 Running the tests

```
pyspark < src/delta-bronze.py
```

### Results of delta-bronze-<table_name>

![img](https://github.com/carlosbpy/py2spark-3.1.1-pgsql/blob/main/docs/raw-zone.png)

## processing-zone.py
the spark job processing-zone.py reads the data incrementally from the raw-zone and writes to the processing-zone to avoid complete processing of the data
## 🔧 Running the tests

```
pyspark < src/processing-zone.py
```

### Results of processing-zone
![img](https://github.com/carlosbpy/pyspark-3.1.1-pgsql/blob/main/docs/processing-zone.png)

## consumer-zone.py
the spark consumer-zone job partitions the data by genre in order to seek better performance for the business analytical layer, metrics are also generated from how many male and female users are on the platform
## 🔧 Running the tests

```
pyspark < src/consumer-zone.py
```

### Results of consumer-zone
![img](https://github.com/carlosbpy/pyspark-3.1.1-pgsql/blob/main/docs/consumer-zone.png)


## ⛏️ Built Using <a name = "built_using"></a>

- [Pyspark](https://spark.apache.org/docs/latest/api/python/index.html) - 3.1.1
- [Postgres](https://hub.docker.com/_/postgres) - Database

## ✍️ Authors <a name = "authors"></a>

- [@carlosbpy](https://github.com/carlosbpy) - Idea & Initial work