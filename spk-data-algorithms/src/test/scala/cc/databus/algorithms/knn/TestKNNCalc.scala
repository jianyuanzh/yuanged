package cc.databus.algorithms.knn

import org.apache.commons.lang.StringUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

/**
  * Created by yuange.zjy on 2019/5/17.
  */
class TestKNNCalc {

  @Test
  def test(): Unit = {
    val spark = getSpark
    val resourceDir = System.getProperty("user.dir") + "/src/test/resources/knn"
    val queryRdd = spark.textFile(s"$resourceDir/R.txt")
      .filter(s => StringUtils.isNotBlank(s))
      .map(l => l.split(";"))
      .filter(lar => lar.length == 2)
//      .map(l => ) TODO
    val trainedRdd = spark.textFile(s"$resourceDir/S.txt")
  }

  private def getSpark: SparkContext = {
    val sparkConf = new SparkConf()
      .setAppName("Test Knn")
      .setMaster("local[4]")

    SparkContext.getOrCreate(sparkConf)
  }
}
