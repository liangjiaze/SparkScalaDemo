package cn.jade.wordCount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val sparkconf:SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
    val sc = new SparkContext(sparkconf)
    sc.setLogLevel("WARN")

    sc.textFile("D:\\bigdata\\data\\words.txt").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).sortBy(_._2,false).collect().foreach(println)

//    val data:RDD[String] = sc.textFile("D:\\bigdata\\data\\words.txt")
//    val words:RDD[String] = data.flatMap(_.split(" "))
//    val wordAndOne:RDD[(String,Int)] =words.map((_,1))
//    val result:RDD[(String,Int)] = wordAndOne.reduceByKey(_+_)
//    val sortResult:RDD[(String,Int)] = result.sortBy(_._2,false)
//    val finalResult:Array[(String,Int)] = sortResult.collect()
//    finalResult.foreach(x => println(x))



    sc.stop()
  }
}
