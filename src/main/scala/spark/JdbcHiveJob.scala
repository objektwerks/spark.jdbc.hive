package spark

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object JdbcHiveJob {
  def main(args: Array[String]): Unit = {
    val logger = Logger.getLogger(getClass.getSimpleName)
    val conf = ConfigFactory.load("job.conf").getConfig("job")

    makeSparkEventLogDir(conf.getString("spark.eventLog.dir"))
    runJob(logger, conf)
  }

  def runJob(logger: Logger, conf: Config): Unit = {
    val sparkConf = new SparkConf()
      .setMaster(conf.getString("master"))
      .setAppName(conf.getString("name"))
      .set("spark.serializer", conf.getString("spark.serializer"))
      .set("spark.eventLog.enabled", conf.getBoolean("spark.eventLog.enabled").toString)
      .set("spark.eventLog.dir", conf.getString("spark.eventLog.dir"))

    val sparkSession = SparkSession
      .builder
      .config(sparkConf)
      .getOrCreate()
    logger.info("*** JdbcHiveJob Spark session built. Press Ctrl C to stop.")

    sys.addShutdownHook {
      sparkSession.stop
      logger.info("*** JdbcHiveJob Spark session stopped.")
    }

    // import sparkSession.implicits._

    // Build spark job.

    ()
  }
}