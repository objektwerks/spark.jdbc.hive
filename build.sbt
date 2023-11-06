name := "spark.jdbc.hive"
organization := "objektwerks"
version := "0.1"
scalaVersion := "2.13.12"
libraryDependencies ++= {
  val sparkVersion = "3.5.0"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
    "org.apache.spark" %% "spark-streaming" % sparkVersion % Provided,
    "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
    "org.apache.spark" %% "spark-hive" % sparkVersion % Provided,
    "com.typesafe" % "config" % "1.4.2",
    "com.oracle.ojdbc" % "ojdbc8" % "19.3.0.0",
    "com.h2database" % "h2" % "2.2.224" % Test,
    "org.scalikejdbc" %% "scalikejdbc" % "4.1.0" % Test,
    "org.scalatest" %% "scalatest" % "3.2.17" % Test
  )
}
