package spark

import org.apache.spark.sql.{Dataset, Encoders, SaveMode}
import org.scalatest.{FunSuite, Matchers}

case class KeyValue(key: Int, value: Int)

object KeyValue {
  val keyValueSchema = Encoders.product[KeyValue].schema
  implicit def keyValueOrdering: Ordering[KeyValue] = Ordering.by(_.key)
}

class JdbcHiveTest extends FunSuite with Matchers {
  import SparkInstance._
  import sparkSession.implicits._

  test("hive") {
    sqlContext.sql("DROP TABLE IF EXISTS keyvalue")
    sqlContext.sql("CREATE TABLE keyvalue (key INT, value INT) row format delimited fields terminated by ','")
    sqlContext.sql("LOAD DATA LOCAL INPATH './target/hive/kv.txt' INTO TABLE keyvalue")
    val keyvalues = sqlContext.sql("SELECT * FROM keyvalue").as[KeyValue].cache
    keyvalues.count shouldBe 9
    keyvalues.filter(_.key == 3).head.value shouldBe 33
    keyvalues.show
  }

  test("jdbc") {
    val tableName = "key_values"
    writeKeyValues(tableName, List[KeyValue](KeyValue(1, 1), KeyValue(2, 2), KeyValue(3, 3)).toDS)
    val keyvalues = readKeyValues(tableName).toDF
    keyvalues.createOrReplaceTempView(tableName)
    sqlContext.sql(s"select count(*) as total_rows from $tableName").head.getLong(0) shouldBe 3
    sqlContext.sql(s"select min(key) as min_key from $tableName").head.getInt(0) shouldBe 1
    sqlContext.sql(s"select max(value) as max_value from $tableName").head.getInt(0) shouldBe 3
    sqlContext.sql(s"select sum(value) as sum_value from $tableName").head.getLong(0) shouldBe 6
  }

  private def writeKeyValues(tableName: String, keyValues: Dataset[KeyValue]): Unit = {
    keyValues
      .write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("driver", "org.h2.Driver")
      .option("url", "jdbc:h2:mem:kv;DB_CLOSE_DELAY=-1")
      .option("user", "sa")
      .option("password", "sa")
      .option("dbtable", tableName)
      .save
  }

  private def readKeyValues(tableName: String): Dataset[KeyValue] = {
    sqlContext
      .read
      .format("jdbc")
      .option("driver", "org.h2.Driver")
      .option("url", "jdbc:h2:mem:kv;DB_CLOSE_DELAY=-1")
      .option("user", "sa")
      .option("password", "sa")
      .option("dbtable", tableName)
      .load
      .as[KeyValue]
  }
}