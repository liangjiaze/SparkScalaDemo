package cn.jade.RDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object PV {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("PV").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val data:RDD[String] = sc.textFile("D:\\bigdata\\data\\access.log")
    println(data.count())
    sc.stop()

  }
}
