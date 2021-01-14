package cn.jade.SparkStreaming

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.{SparkConf, SparkContext}


object SparkStreamingTCPWindowHotWords {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("SparkStreamingTCPWindowHotWords ").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    val ssc: StreamingContext = new StreamingContext(sc,Seconds(5))
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("node1",9999)
    val wordAndOne: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_,1))
    val result: DStream[(String, Int)] = wordAndOne.reduceByKeyAndWindow((a:Int,b:Int)=>a+b,Seconds(10),Seconds(5))
    val data: DStream[(String, Int)] = result.transform(rdd => {
      val dataRDD: RDD[(String, Int)] = rdd.sortBy(t => t._2, false)
      val sortResult: Array[(String, Int)] = dataRDD.take(3)
      println("--------------print top 3 begin--------------")
      sortResult.foreach(println)
      println("--------------print top 3 end--------------")
      dataRDD
    })
    data.print()
    ssc.start()
    ssc.awaitTermination()

  }

}
