# Problem 6: Hive vs Pig Comparison Write-up

### 1. Development Effort
Hive uses standard SQL syntax (HQL), making analytical queries quick and intuitive to construct. However, implementing specialized custom functions requires creating and compiling a Java UDF class. Pig uses procedural data-flow scripting (Pig Latin), which allows developers to build step-by-step transformation pipelines and write inline logic without external UDF compilations.

### 2. Code Readability
Hive offers superior code readability for engineers accustomed to SQL relational queries and database operations. Pig scripts require managing relation field prefixes (e.g., `yield_data::region`), which makes complex relational joins and dereferencing more verbose.

### 3. Execution Time & Performance
Hive’s ability to store datasets in optimized columnar formats like RCFile significantly reduces disk I/O during joins. While both frameworks convert scripts into MapReduce tasks, Hive's columnar storage format gives it a performance advantage for analytical join queries.

### Recommendation
**Apache Hive** is recommended for this type of agricultural data analysis due to its declarative SQL syntax, native support for optimized columnar storage formats like RCFile, and clean aggregation capabilities across large datasets.
