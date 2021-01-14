package cn.jade.RDD

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object TopN {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("PV").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val data:RDD[String] = sc.textFile("D:\\bigdata\\data\\access.log")
    val urlAndOne: RDD[(String, Int)] = data.map(_.split(" ")).filter(_.length>10).map(x=>(x(10),1))
    val urlTotalNum: RDD[(String, Int)] = urlAndOne.reduceByKey(_+_)
    val sortUrl:RDD[(String,Int)]=urlTotalNum.sortBy(_._2,false)
    val result:Array[(String,Int)] = sortUrl.take(5)
    result.foreach(x=>println(x))
    sc.stop()


  }

}
