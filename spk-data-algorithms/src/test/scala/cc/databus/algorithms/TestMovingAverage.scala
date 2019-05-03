package cc.databus.algorithms

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.commons.lang3.StringUtils
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.junit.Test

class TestMovingAverage {

  lazy val preparedRdd = getSpark.textFile(s"${System.getProperty("user.dir")}/src/test/resources/stock_series.txt")
    .filter(StringUtils.isNoneEmpty(_))
    .map(line => line.split(","))
    .filter(arr => arr.length == 3)
    .mapPartitions {
      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      iter: Iterator[Array[String]] => {
        iter.map(arr => (arr(0), sdf.parse(arr(1)).getTime, arr(2).toDouble))
      }
    }


  def getSpark: SparkContext = {
    val sparkConf = new SparkConf()
      .setAppName("TestMovingAverage")
      .setMaster("local[4]")

    SparkContext.getOrCreate(sparkConf)
  }

  @Test
  def test(): Unit = {
    val ws = getSpark.broadcast(2)

    val rdd = MovingAverage.average(preparedRdd, ws)
    val collected = rdd
      .collect()

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    for (m <- collected) {
      val key = m._1
      for (p <- m._2) {
        println(s"$key, ${sdf.format(new Date(p._1))}, ${p._2}")
      }
    }

  }

  @Test
  def testSortInMemory(): Unit = {
    val ws = getSpark.broadcast(2)

    val rdd = MovingAverage.averageInMemory(preparedRdd, ws)
    val collected = rdd
      .collect()

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    for (m <- collected) {
      val key = m._1
      for (p <- m._2) {
        println(s"$key, ${sdf.format(new Date(p._1))}, ${p._2}")
      }
    }
  }
}
