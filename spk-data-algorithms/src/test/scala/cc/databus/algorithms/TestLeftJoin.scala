package cc.databus.algorithms

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

/**
  * Created by yuange.zjy on 2019/4/30.
  */
class TestLeftJoin {

  /**
    * Format of users:
    * |user_id|location|
    *  ----------------
    * | u1    | UT     |
    *
    * Format of transactions:
    * |trans id | prod id | userId |quantity | amount |
    * |t2       |     p1  |   u2   |     1   |  400   |
    */
  @Test
  def test(): Unit ={
    val spark = getSpark()
    val resourceDir = System.getProperty("user.dir") + "/src/test/resources"
    val users = spark.textFile(s"$resourceDir/users.txt")
      .map(l => l.split(" "))
      .map(arr => arr(0) -> ("L", arr(1)))

    val transactions = spark.textFile(s"$resourceDir/transactions.txt")
      .filter(_.nonEmpty)
      .map(l => l.split(" "))
      .map(arr => (arr(2), ("P", arr(1))))

    val joined = users.union(transactions)
      .groupByKey()
      .flatMap (
        {
          case (uid: String, iter: Iterable[(String, String)]) =>
            val typeValues = iter.groupBy(_._1)

            val locations = typeValues.getOrElse("L", Iterable.empty)
              .map(m => m._2).toSeq

            val products = typeValues.getOrElse("P", Iterable.empty)
              .map(m => m._2)


            products.flatMap(p => {
              locations.map(l => (p, l))
            })
        }
      )
      .groupByKey()


    joined.collect().foreach(println)
  }

  @Test
  def testSpkRddLeftJoin(): Unit = {
    val spark = getSpark()
    val resourceDir = System.getProperty("user.dir") + "/src/test/resources"

    val users = spark.textFile(s"$resourceDir/users.txt")
      .map(l => l.split(" "))
      .map(arr => arr(0) ->  arr(1))

    val transactions = spark.textFile(s"$resourceDir/transactions.txt")
      .filter(_.nonEmpty)
      .map(l => l.split(" "))
      .map(arr => (arr(2), arr(1)))

    users.leftOuterJoin(transactions)
      .map(m => (m._2._2.getOrElse(""), m._2._1))
      .filter(m => m._1.nonEmpty)
      .groupByKey()
      .collect()
      .foreach(println)


  }


  def getSpark(): SparkContext = {
    val sparkConf = new SparkConf()
      .setAppName("Test LeftJoin")
      .setMaster("local[4]")

    SparkContext.getOrCreate(sparkConf)
  }

}
