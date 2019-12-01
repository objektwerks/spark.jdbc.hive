package spark

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger

object SparkInstance {
  val logger = Logger.getLogger(getClass.getSimpleName)
  val conf = ConfigFactory.load("test.conf").getConfig("job")

  makeSparkEventLogDir(conf.getString("spark.eventLog.dir"))

  // Fixes Derby AccessControl Exception: org.apache.derby.security.SystemPermission( "engine", "usederbyinternals" )
  System.setSecurityManager(null)

  val sparkSession = SparkSession
    .builder
    .appName(conf.getString("name"))
    .master(conf.getString("master"))
    .config("spark.serializer", conf.getString("spark.serializer"))
    .config("spark.eventLog.enabled", conf.getBoolean("spark.eventLog.enabled"))
    .config("spark.eventLog.dir", conf.getString("spark.eventLog.dir"))
    .enableHiveSupport
    .getOrCreate()
  val sparkContext = sparkSession.sparkContext
  val sqlContext = sparkSession.sqlContext
  logger.info("*** Initialized Spark instance.")

  sys.addShutdownHook {
    sparkSession.stop()
    logger.info("*** Terminated Spark instance.")
  }
}