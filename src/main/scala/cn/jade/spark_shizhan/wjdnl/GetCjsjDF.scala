package cn.jade.spark_shizhan.wjdnl

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}
import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat

object GetCjsjDF {

  case class Dlb(dlbbs: String, cjsj: String, adl: String, bdl: String, cdl: String)

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getName)
    val sparkSession: SparkSession = SparkSession.builder()
      .config(conf = sparkConf)
      .getOrCreate()
    val sc: SparkContext = sparkSession.sparkContext
    sc.setLogLevel("WARN")

    val array = Array("adl","bdl","cdl")
    val data: RDD[String] = sc.textFile("file:///E:/bigdata/data/test.txt")
    val dlbbsRdd: RDD[Array[String]] = data.map(_.split(","))

    val DlbRDD: RDD[Dlb] = dlbbsRdd.map(x => Dlb(x(0), x(1), x(2), x(3), x(4)))
    import sparkSession.implicits._
    val DLbDF: DataFrame = DlbRDD.toDF

    DLbDF.createTempView("t_dlbbs")

//    array.foreach(itm => {
//      println(itm)
//    })

    val dlbbsAvg: RDD[Row] = DLbDF.groupBy("dlbbs").agg(("adl", "avg"), ("bdl", "avg"), ("cdl", "avg")).rdd
    dlbbsAvg.foreach(println)
    //    val adlAvg = DLbDF.groupBy("dlbbs").agg(("adl", "avg")).rdd
    //    val bdlAvg = DLbDF.groupBy("dlbbs").agg(("bdl", "avg")).rdd
    //    val cdlAvg = DLbDF.groupBy("dlbbs").agg(("cdl", "avg")).rdd
    //    adlAvg.foreach(println)
    //    bdlAvg.foreach(println)
    //    cdlAvg.foreach(println)

    //    dlbbsAdlAvg.cogroup(idScore).collect().foreach(println)
    //    dlbbsAdlAvg.fullOuterJoin(idScore).collect().foreach(println)

    val groups: RDD[(String, String, Float, Float, Float)] = data.map(_.split(",")).map(dlbb => (dlbb(0), dlbb(1), dlbb(2).toFloat, dlbb(3).toFloat, dlbb(4).toFloat))


    val dlbss: RDD[(String, Map[String, List[String]])] = groups.groupBy(item => item._1).map(dlb => {
      val dlbbs: String = dlb._1
      array.foreach(ftak =>{
        for (i <- 3 until 9){
          val adlMax: List[String] = dlb._2.toList.sortBy(_._3)(Ordering.Float.reverse).take(1).map(item => item._3 + ";" + item._2)
          val adlMin: List[String] = dlb._2.toList.sortBy(_._3)(Ordering.Float).take(1).map(item => item._3 + ";" + item._2)
        }

      })
      val adlMin = dlb._2.toList.sortBy(_._3)(Ordering.Float).take(1).map(item => item._3 + ";" + item._2)
      val bdlMin = dlb._2.toList.sortBy(_._4)(Ordering.Float).take(1).map(item => item._4 + ";" + item._2)
      val cdlMin = dlb._2.toList.sortBy(_._5)(Ordering.Float).take(1).map(item => item._5 + ";" + item._2)
      val adlMax = dlb._2.toList.sortBy(_._3)(Ordering.Float.reverse).take(1).map(item => item._3 + ";" + item._2)
      val bdlMax = dlb._2.toList.sortBy(_._4)(Ordering.Float.reverse).take(1).map(item => item._4 + ";" + item._2)
      val cdlMax = dlb._2.toList.sortBy(_._5)(Ordering.Float.reverse).take(1).map(item => item._5 + ";" + item._2)

      (dlbbs, Map(
        "adlMin" -> adlMin, "bdlMax" -> bdlMin, "cdlMin" -> cdlMin,
        "adlMax" -> adlMax, "bdlMax" -> bdlMax, "cdlMax" -> cdlMax
        //        "adlAvg" -> dlbbsAvg.take(1), "bdlAvg" -> dlbbsAvg.take(2), "cdlAvg" -> dlbbsAvg.take(3)
      ))
    })
    //    dlbss.join("dlbbsAdlAvg",dlbbsAdlAvg.)
    dlbss.foreach(println)



    //    DLbDF.show()

    //    val dlbbsAdlDF = DLbDF.groupBy("dlbbs").agg(("adl", "min"), ("adl", "max"), ("adl", "avg"))
    //    dlbbsAdlDF.show()


    //    val dlbbsAdlDFMin = DLbDF.groupBy("dlbbs").agg(("adl","min")).show()
    //    val dlbbsAdlDFMax = DLbDF.groupBy("dlbbs").agg(("adl","max")).show()
    //    val dlbbsAdlDFAvg = DLbDF.groupBy("dlbbs").agg(("adl", "avg")).show()


    //    val dlbbsAdlDFMin = DLbDF.groupBy("dlbbs").agg(functions.min("adl").alias("adl"))
    //    val dlbbsAdlDFMax = DLbDF.groupBy("dlbbs").agg(functions.max("adl").alias("adl"))
    //    val dlbbsAdlDFAvg = DLbDF.groupBy("dlbbs").agg(functions.avg("adl").alias("adl"))
    //    dlbbsAdlDFMin.show()
    //    dlbbsAdlDFMax.show()
    //    dlbbsAdlDFAvg.show()

    //    val dlbbsAdlDFMinCjsj = DLbDF.join(dlbbsAdlDFMin, Seq("dlbbs", "adl")).select("dlbbs", "adl", "cjsj")
    //    val dlbbsAdlDFMaxCjsj = DLbDF.join(dlbbsAdlDFMax, Seq("dlbbs", "adl")).select("dlbbs", "adl", "cjsj")

    //    dlbbsAdlDFMinCjsj.sort($"cjsj".asc)


    //    val dlbbsAdlDFMinCjsjMin = DLbDF.join(dlbbsAdlDFMin, DLbDF("dlbbs") === dlbbsAdlDFMin("dlbbs") && DLbDF("adl") === dlbbsAdlDFMin("adl"))

    //    val tm = s"$dlbbsAdlDFMinCjsj.cjsj"
    //    val a = tranTimeToLong(tm)
    //    println(a)


    //    val rowRdd = dlbbsAdlDFMinCjsj.rdd.map(attributes => Row(attributes(0), attributes(1), attributes(2))).foreach(println)


    def printList(list: List[_]): Unit = {
      list.foreach(elem => println(elem + " "))
    }


    /*val pcsKV = data.map(_.split(",")).map { x =>
      val key = x(0)
      val value = x(1) + ";" + x(2) + ";" + x(3) + ";" + x(4)
      (key, value)
    }
    pcsKV.foreach(println)*/


    //    val dlbbsAdlDF = DLbDF.groupBy("dlbbs").agg(("adl","min"),("adl","max"),("adl","avg"))
    //    dlbbsAdlDF.show()

    //    val dlbbsAdlDFMin = DLbDF.groupBy("dlbbs").agg(("adl","min"))
    val dlbbsAdlDFMin = DLbDF.groupBy("dlbbs").agg(functions.min("adl").alias("adl"))
    //    dlbbsAdlDFMin.show()

    //    val dlbbsAdlDFMin = DLbDF.groupBy("dlbbs").agg(min("adl"))
    val dlbbsAdlDFMinCjsj = DLbDF.join(dlbbsAdlDFMin, Seq("dlbbs", "adl")).select("dlbbs", "adl", "cjsj")
    //    val dlbbsAdlDFMinCjsj = DLbDF.join(dlbbsAdlDFMin, Seq("*")).select("dlbbs","adl","cjsj")
    //    val dlbbsAdlDFMinCjsj = DLbDF.join(dlbbsAdlDFMin).join(dlbbsAdlDFMin, DLbDF("dlbbs")===dlbbsAdlDFMin("dlbbs") && DLbDF("adl")=== dlbbsAdlDFMin("adl"))
    val dlbbsAdlDFMax = DLbDF.groupBy("dlbbs").agg(("adl", "max"))
    val dlbbsAdlDFAvg = DLbDF.groupBy("dlbbs").agg(("adl", "avg"))
    //    dlbbsAdlDFMinCjsj.show()
    val dlbbsAdlDFMinCjsjMin = dlbbsAdlDFMinCjsj.sortWithinPartitions("cjsj") take (1)


    val dlbbsBdlDF = DLbDF.groupBy("dlbbs").agg(("bdl", "min"), ("bdl", "max"), ("bdl", "avg"))
    //    dlbbsBdlDF.show()
    val dlbbsCdlDF = DLbDF.groupBy("dlbbs").agg(("cdl", "min"), ("cdl", "max"), ("cdl", "avg"))
    //    dlbbsCdlDF.show()


    def tranTimeToLong(tm: String): Long = {
      val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val dt = fm.parse(tm)
      val aa = fm.format(dt)
      val tim: Long = dt.getTime()
      tim
    }

    sc.stop()
    sparkSession.stop()


  }
}
