package cc.databus.algorithms

import org.apache.spark.HashPartitioner
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object MovingAverage {

  /**
    * Average with giving window
    *
    * @param rdd input rdd
    * @param windowSize broadcast window size
    * @return
    */
  def averageInMemory(rdd: RDD[(String, Long, Double)], windowSize: Broadcast[Int]): RDD[(String, Iterable[(Long, Double)])] = {
    require(rdd != null, "input rdd should not be null")
    rdd.map { case (key: String, ts: Long, v: Double) => (key, (ts, v)) }
      .groupByKey()
      .mapValues {
        iter: Iterable[(Long, Double)] =>
          val sorted = iter.toSeq.sortBy(_._2)
          val wSize = windowSize.value
          val queue = new mutable.Queue[Double]
          for (v <- sorted) yield {
            queue.enqueue(v._2)
            if (queue.size > wSize) {
              queue.dequeue()
            }
            (v._1, queue.sum / queue.size)
          }
      }
  }


  def average(rdd: RDD[(String, Long, Double)], windowSize: Broadcast[Int]): RDD[(String, Iterable[(Long, Double)])] = {
    require(rdd != null, "input rdd should not be null")

    rdd.map { case (key: String, ts: Long, v: Double) => CompositeKey(key, ts) -> TimeSeriesData(ts, v) }
      .repartitionAndSortWithinPartitions(new CompositeKeyPartitioner(4))
      .map(m => m._1.key -> m._2)
      .groupByKey()
      .mapValues(values => {
        val wSize = windowSize.value
        val queue = new mutable.Queue[Double]()
        for (tsd <- values) yield {
          queue.enqueue(tsd.value)
          if (queue.size > wSize) {
            queue.dequeue()
          }
          (tsd.timestamp, queue.sum / queue.size)
        }
      })

  }
}

case class TimeSeriesData(timestamp: Long, value: Double) {}

case class CompositeKey(key: String, timestamp: Long) {}

object CompositeKey {
  implicit def ordering[A <: CompositeKey]: Ordering[A] = {
    Ordering.by(fk => (fk.key, fk.timestamp))
  }
}

class CompositeKeyPartitioner(partitions: Int) extends HashPartitioner(partitions) {
  override def getPartition(key: Any): Int = {
    key match {
      case k : CompositeKey => Math.abs(k.key.hashCode % numPartitions)
      case o : Any => super.getPartition(o)
    }
  }
}

