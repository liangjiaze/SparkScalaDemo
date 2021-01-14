package cn.jade.RDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object UV {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("PV").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val data:RDD[String] = sc.textFile("E:\\bigdata\\data\\access.log")
    val ips:RDD[String] = data.map(_.split(" ")(0))
    val distinctIps:RDD[String] = ips.distinct()
    println(distinctIps.count())
    sc.stop()


  }
}
