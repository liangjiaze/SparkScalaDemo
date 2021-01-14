package cn.jade.spark_shizhan.wjdnl

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object GetCjsjRDD {
  def main(args: Array[String]): Unit = {

    def printList(list: List[_]): Unit = {
      list.foreach(elem => println(elem + " "))
    }

    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getName)
    val sparkSession = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()
    val sc: SparkContext = sparkSession.sparkContext
    sc.setLogLevel("WARN")
    val data: RDD[String] = sc.textFile("file:///E:/bigdata/data/test.txt")

    data.foreach(println)

    val pcsKV = data.map(_.split(",")).map { x =>
      val key: String = x(0)
//      val value: String = x(1) + ";" + x(2) + ";" + x(3) + ";" + x(4)
      val  minData: Unit =
      for(i <- 2 until  x.length){
        val MinTime: List[String] = x(1).toList.sortBy(_.unary_+)(Ordering.Int).take(1).map(item =>x(i)+":"+ x(1))
//        printList(MinTime)
      }

      (key,minData)
    }

    val groups: RDD[(String, String, Float, Float, Float)] = data.map(_.split(",")).map(dlbb => (dlbb(0), dlbb(1), dlbb(2).toFloat, dlbb(3).toFloat, dlbb(4).toFloat))


    //    val dlbss = groups.groupBy(item => item._1).map(dlb => {
    //      val dlbbs = dlb._1
    //      val adlMin = dlb._2.toList.sortBy(_._3)(Ordering.Float).take(1).map(item => item._3 + "," + item._2)
    //      val adlMax = dlb._2.toList.sortBy(_._3)(Ordering.Float.reverse).take(1).map(item => item._3 + "," + item._2)
    //
    //      (dlbbs, Map("adlMin" -> adlMin, "adlMax" -> adlMax))
    //    })
    //    dlbss.foreach(println)
    //    val dlbs = groups.groupBy(item => item._1).collect()
    //    dlbs.foreach(println)

    val dlbss = groups.groupBy(item => item._1).map(dlb => {
      val dlbbs = dlb._1
      val dataMin = dlb._2.toList.sortBy(_._3)(Ordering.Float).take(1).map(item => item._3 + "," + item._2)
      val dataMax = dlb._2.toList.sortBy(_._3)(Ordering.Float.reverse).take(1).map(item => item._3 + "," + item._2)
      val dataAvg =
        (dlbbs, Map("adlMin" -> dataMin, "adlMax" -> dataMax))
    })
    dlbss.foreach(println)



    sc.stop()
    sparkSession.stop()
  }
}
