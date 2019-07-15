# migrate_oracle_table_to_aerospike

Use an ETL tool like spark that gets data out of Oracle table and puts it in aerospike set. It can use JDBC connection to Oracle instance to read data and aerospike-spark connector to load data into Aerospike. You need the jar file for the database and aerospike-spark connect license. Again this will provide a jar file. Getting data out of MongoDB document into Aerospike is pretty straight forward with Spark. Practically every NoSQL vendor has a Spark connector module.

```
Need jdbc driver for Oracle like ojdbc6.jar.
Need arospike-spark connector license for Enterprise edition of Spark.
Need the jar file for the above connector like aerospike-spark-assembly-1.1.2.jar.
```
Need to ensure that every node of Spark cluster has aerospike keyPath in the same place. i.e. -->    
```
spark.conf.set("aerospike.keyPath", "/etc/aerospike/features.conf")
```
Otherwise you are going to get an error!

