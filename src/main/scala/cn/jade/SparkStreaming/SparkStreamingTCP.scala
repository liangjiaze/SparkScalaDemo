package cn.jade.SparkStreaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


object SparkStreamingTCP {
  def main(args: Array[String]): Unit = {
    val sparkConf:SparkConf = new SparkConf().setAppName("SparkStreamingTCP ").setMaster("local[2]")
    val sc:SparkContext = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    val ssc: StreamingContext = new StreamingContext(sc,Seconds(5))
    val lines:ReceiverInputDStream[String] = ssc.socketTextStream("node1",9999)
    val words:DStream[String] = lines.flatMap(_.split(" "))
    val wordAndOne:DStream[(String,Int)] = words.map((_,1))
    val result: DStream[(String, Int)] = wordAndOne.reduceByKey(_+_)
    result.print()
    ssc.start()
    ssc.awaitTermination()

  }

}
