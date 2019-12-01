name := "spark.jdbc.hive"
organization := "objektwerks"
version := "0.1-SNAPSHOT"
scalaVersion := "2.11.12"
libraryDependencies ++= {
  val sparkVersion = "2.4.0"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-streaming" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "com.h2database" % "h2" % "1.4.200",
    "com.typesafe" % "config" % "1.3.4",
    "org.scalikejdbc" %% "scalikejdbc" % "3.4.0",
    "com.oracle.ojdbc" % "ojdbc8" % "19.3.0.0",
    "org.scalatest" %% "scalatest" % "3.0.8" % Test
  )
}