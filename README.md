# SamzaSQL: Fast Data Management with Streaming SQL and Apache Samza

SamzaSQL is a scalable and fault-tolerant SQL based streaming query engine implemented on top of Apache Samza with support for interaction with non-streaming data sources. SamzaSQL define a minimal set of extensions to standard SQL to enable fast data querying and data manipulation.  Queries expressed in streaming SQL are compiled into physical plans that are scalable, fault-tolerant  and executed on *[Apache Samza](http://samza.apache.org)*. SamzaSQL front-end, which does query parsing, validation and planning is built on top of *[Apache Calcite](http://calcite.apache.org)*. SamzaSQL is architectured to support variuos data formats such as Avro or JSON and variuos traditional relational and NoSQL stores using plugable extensions. SamzaSQL provides a window operator implementation that provides timely and deterministic window output under unpredictable message delivery latencies, and deterministic window output with node failures and message re-delivery.

**Please note that this repository contains a copy of streaming SQL related components from Samza's samza-sql branch in addition to the features I added over them. I am planning to contribute new features and improvements back to Samza once this project become stable.**

## License

Apache License 2.0


