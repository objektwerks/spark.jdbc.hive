Spark Jdbc Hive Job
-------------------
>Job that sources a Jdbc store, transforms values and sinks to a Hive store.

Source
------
1. Jdbc store with keyvalue table, with key(int) and value(int) columns.

Flow
----
1. Transform source keyvalue to new keyvalue by incrementing value + 1.

Sink
----
1. Hive store with keyvalue table, with key(int) and value(int) columns.

Test
----
1. sbt clean test

Run
---
1. sbt clean compile run
2. Press Ctrl C to stop.

Submit
------
>First create a log4j.properties file from log4j.properties.template.
>See: /usr/local/Cellar/apache-spark/2.4.4s/libexec/conf/log4j.properties.template

1. sbt clean compile package
2. chmod +x submit.sh ( required only once )
3. ./submit.sh
4. Press Ctrl C to stop.

>**NOTE** The 2 commandline args are specified in submit.sh.

UI
--
1. SparkUI : localhost:4040
2. History Server UI : localhost:18080 : start-history-server.sh | stop-history-server.shs
 
Log
---
1. ./target/app.log

Events
------
1. /tmp/spark-events