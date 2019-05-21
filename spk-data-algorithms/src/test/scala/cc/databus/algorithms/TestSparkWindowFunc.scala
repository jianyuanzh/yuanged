package cc.databus.algorithms

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.junit.Test

/**
  * Created by yuange.zjy on 2019/5/21.
  */
class TestSparkWindowFunc {

  lazy val session = new SparkSession.Builder()
    .appName("Spark window func")
    .master("local[4]")
    .getOrCreate()

  def loadCsv(): Dataset[Row] = {
    val path = System.getProperty("user.dir") + "/src/test/resources/data.csv"

    session.sqlContext.read.format("com.databricks.spark.csv")
      .option("header", true)
      .option("inferSchema", true)
      .load(path)
  }

  @Test
  def test(): Unit = {
    val ds = loadCsv()
    ds.createTempView("tmp_view")

//    select device_sign, user_id, size(users) user_n, total_users from
//    (

    //    ) a

    val result = session.sql(
      """
        |SELECT device_sign, user_id, users, avgUsers, size(uni_users) uni_users_n from
        |(
        |   SELECT
        |     device_sign,user_id,
        |      sum(user_id) over(partition by device_sign) as users,
        |      avg(user_id) over(partition by device_sign) as avgUsers,
        |      collect_set(user_id) over(partition by device_sign) as uni_users
        |   from tmp_view
        |) a
      """.stripMargin)

    println(
      """
        |SELECT device_sign, user_id, users, avgUsers, size(uni_users) uni_users_n from
        |(
        |   SELECT
        |     device_sign,user_id,
        |      sum(user_id) over(partition by device_sign) as users,
        |      avg(user_id) over(partition by device_sign) as avgUsers,
        |      collect_set(user_id) over(partition by device_sign) as uni_users
        |   from tmp_view
        |) a
      """.stripMargin)
    println(result.queryExecution.executedPlan.toString())

//    result.toString()
    val direct_result = session.sql(
      """
        |select a.user_id, m.* from
        |tmp_view a
        |join
        |( select
        |   device_sign,
        |   sum(user_id) total_users,
        |   avg(user_id) avgusers,
        |   count(distinct user_id) uni_users
        | from tmp_view group by device_sign) m
        |on a.device_sign=m.device_sign
        |
            """.stripMargin)

    println("""
              |select a.user_id, m.* from
              |tmp_view a
              |join
              |( select
              |   device_sign,
              |   sum(user_id) total_users,
              |   avg(user_id) avgusers,
              |   count(distinct user_id) uni_users
              | from tmp_view group by device_sign) m
              |on a.device_sign=m.device_sign
              |
            """.stripMargin)
    println(direct_result.queryExecution.executedPlan.toString())

//    result.show(100)
  }
}
