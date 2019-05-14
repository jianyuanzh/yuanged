package cc.databus

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}

/**
  * Created by yuange.zjy on 2019/5/14.
  */
class WorldCountStreaming {

  def run(ssc: StreamingContext): Unit = {
    val lines = ssc.socketTextStream("localhost", 9999)
    val workdCounts = lines.flatMap(_.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    workdCounts.print()
  }



}
