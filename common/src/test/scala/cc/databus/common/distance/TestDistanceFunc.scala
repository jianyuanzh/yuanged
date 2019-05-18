package cc.databus.common.distance

import org.junit.{Assert, Test}

/**
  * Created by yuange.zjy on 2019/5/14.
  */
class TestDistanceFunc {
  val inputSet = Array(
    (Seq(1.0), Seq(2.0), 1.0),
    (Seq(0.0,0.0), Seq(2.0, 2.0), 2.83),
    (Seq(1.0,2.0), Seq(3.0,2.0), 2.00)
  )

  @Test
  def test(): Unit = {
    for (s <- inputSet) {
      val expected = s._3
      val rst = DistanceFunc.euclidian(s._1, s._2)
      Assert.assertEquals(s"references=${s._1}, query=${s._2}", "%.2f".format(expected), "%.2f".format(rst))
    }
  }


}
