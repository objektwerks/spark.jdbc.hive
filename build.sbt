name := "spark.jdbc.hive"
organization := "objektwerks"
version := "0.1"
scalaVersion := "2.11.12"
libraryDependencies ++= {
  val sparkVersion = "2.4.0"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
    "org.apache.spark" %% "spark-streaming" % sparkVersion % Provided,
    "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
    "org.apache.spark" %% "spark-hive" % sparkVersion % Provided,
    "com.typesafe" % "config" % "1.3.4",
    "com.oracle.ojdbc" % "ojdbc8" % "19.3.0.0",
    "com.h2database" % "h2" % "1.4.200" % Test,
    "org.scalikejdbc" %% "scalikejdbc" % "3.4.0" % Test,
    "org.scalatest" %% "scalatest" % "3.0.8" % Test
  )
}