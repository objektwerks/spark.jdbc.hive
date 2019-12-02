package spark

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object JdbcHiveJob {
  def main(args: Array[String]): Unit = {
    val logger = Logger.getLogger(getClass.getSimpleName)
    val conf = ConfigFactory.load("job.conf")

    makeSparkEventLogDir(conf.getString("spark.eventLog.dir"))
    runJob(logger, conf)
  }

  def runJob(logger: Logger, conf: Config): Unit = {
    val sparkConf = new SparkConf()
      .setAppName(conf.getString("job.name"))
      .setMaster(conf.getString("job.master"))
      .set("spark.serializer", conf.getString("job.spark.serializer"))
      .set("spark.eventLog.enabled", conf.getBoolean("job.spark.eventLog.enabled").toString)
      .set("spark.eventLog.dir", conf.getString("job.spark.eventLog.dir"))

    val sparkSession = SparkSession
      .builder
      .config(sparkConf)
      .getOrCreate()
    logger.info("*** JdbcHiveJob Spark session built. Press Ctrl C to stop.")

    sys.addShutdownHook {
      sparkSession.stop
      logger.info("*** JdbcHiveJob Spark session stopped.")
    }

    import sparkSession.implicits._

    sparkSession
      .sqlContext
      .read
      .format("jdbc")
      .option("driver", conf.getString("db.driver"))
      .option("url", conf.getString("db.url"))
      .option("user", conf.getString("db.user"))
      .option("password", conf.getString("db.password"))
      .option("dbtable", conf.getString("db.table"))
      .load
      .as[KeyValue]
      .map(keyvalue => keyvalue.copy(value = keyvalue.value + 1))
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable(conf.getString("hive.table"))
    ()
  }
}