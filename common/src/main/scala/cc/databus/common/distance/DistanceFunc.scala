package cc.databus.common.distance

/**
  * Created by yuange.zjy on 2019/5/14.
  */
object DistanceFunc {
  /**
    * 欧式距离
    * @param query query
    * @param reference reference
    * @return Euclidian distance
    */
  def euclidian(query: Seq[Double], reference: Seq[Double]): Double = {
    require(query.length == reference.length, s"query and reference should have same dimension[query=${query.length}, reference=${reference.length}].")

    val sqrDiff = for (i <- query.indices) yield {
      val diff = query(i) - reference(i)
      diff * diff
    }

    Math.sqrt(sqrDiff.sum)
  }

  /**
    * 欧式距离
    * @param query query
    * @param reference reference
    * @return Euclidian distance
    */
  def euclidian(query: Point, reference: Point): Double = {
    euclidian(query.vector, reference.vector)
  }

}


case class Point(vector: Seq[Double])