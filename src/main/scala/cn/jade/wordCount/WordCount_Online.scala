package cn.jade.wordCount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount_Online {
  def main(args: Array[String]): Unit = {
    val sparkConf:SparkConf = new SparkConf().setAppName("WordCount_Online")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    val data: RDD[String] = sc.textFile(args(0))
    val words:RDD[String] = data.flatMap(_.split(" "))
    val wordAndOne:RDD[(String,Int)] = words.map((_,1))
    val result:RDD[(String,Int)] = wordAndOne.reduceByKey(_+_)
    val sortResult:RDD[(String,Int)] = result.sortBy(_._2,false)
    sortResult.saveAsTextFile(args(1))
    sc.stop()

  }

}
