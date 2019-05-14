package cc.databus

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}
import org.junit.Test

/**
  * Created by yuange.zjy on 2019/5/14.
  *
  * run resources/nc-server.sh before start this case.
  */
class TestWorldCountStreaming {
  @Test
  def test = {
    val sparkConf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("Test WorldCountStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(60))
    new WorldCountStreaming().run(ssc)

    ssc.start()
    ssc.awaitTermination()
  }

  def createStreamingContext(conf: SparkConf, durationInMs: Long): StreamingContext = {
    new StreamingContext(conf, Milliseconds(durationInMs))
  }
}
