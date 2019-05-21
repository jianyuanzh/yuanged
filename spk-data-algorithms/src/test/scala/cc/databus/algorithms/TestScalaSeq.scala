package cc.databus.algorithms

/**
  * Created by yuange.zjy on 2019/5/21.
  */
object TestScalaSeq extends App {
  val aggSqls = Array("agg1", "agg2", "agg3")
  val origins = "origin1, origin2, origin3"


  println(Seq(origins, aggSqls :_ *).mkString(","))
}
