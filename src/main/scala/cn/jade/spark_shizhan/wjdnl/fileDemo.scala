package cn.jade.spark_shizhan.wjdnl

import scala.collection.mutable
import scala.io.Source

object fileDemo {
  def main(args: Array[String]): Unit = {

    val source = Source.fromFile("E://bigdata//data//test.txt").mkString
    val map = new mutable.HashMap[String, String]()
    val map1 = new mutable.HashMap[String, Int]()
    val map2 = new mutable.HashMap[String, String]()
    val map3 = new mutable.HashMap[String, String]()
    val map4 = new mutable.HashMap[String, String]()
    val lines = source.split("\r\n")
    lines.foreach(data => {
      val dlbbs = data.split(",")(0)
      val cjsj = data.split(",")(1)
      val adl = data.split(",")(2)
      val bdl = data.split(",")(3)
      val cdl = data.split(",")(4)
      val Str = cjsj + "," + adl + "," + bdl + "," + cdl

      map.put(Str, dlbbs)
      map1.put(dlbbs, 1)

    })

    val set11 = map1.keySet
    set11.foreach(dat => {
      if (dat.equals("test001")) {

        map.keySet.foreach(adada => {

          val time = adada.split(",")(0)
          val a = adada.split(",")(1)
          val b = adada.split(",")(2)
          val c = adada.split(",")(3)
          map2.put(a, time)
          map3.put(b, time)
          map4.put(c, time)

        })
        println(dat)
      }

    })

    for (key <- map2.keySet) {
      println(key)


      //之后求最大最小值。。。。。。。。。。。。
    }



  }
}
