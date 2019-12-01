#!/bin/sh
spark-submit \
  --class spark.JdbcHiveApp \
  --master local[2] \
  --packages com.typesafe:config:1.3.4, org.scalikejdbc:scalikejdbc:3.4.0, com.oracle.ojdbc:ojdbc8:19.3.0.0 \
  ./target/scala-2.11/spark-jdbc-hive_2.11-0.1-SNAPSHOT.jar