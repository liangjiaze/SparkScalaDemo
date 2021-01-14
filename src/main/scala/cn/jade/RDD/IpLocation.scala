package cn.jade.RDD

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object IpLocation {
  def ip2Long(ip:String):Long = {
    val ips:Array[String] = ip.split("\\.")
    var ipNum:Long = 0L
    for (i <- ips){
      ipNum = i.toLong|ipNum<<8L
    }
    ipNum
  }


  def binarySearch(ipNum: Long, broadcastValue: Array[(String, String, String, String)]):Int = {
    var start = 0
    var end = broadcastValue.length-1
    while(start<=end){      // todo 这个地方是<=
      var middle = (start+end)/2    //todo 之前少括号了  ，逻辑判断不对
      if(ipNum>=broadcastValue(middle)._1.toLong && ipNum<=broadcastValue(middle)._2.toLong){
        return middle
      }
      if(ipNum<broadcastValue(middle)._1.toLong){
        end=middle
      }
      if (ipNum>broadcastValue(middle)._2.toLong){
        start=middle
      }
    }
    -1
  }

  def data2mysql(iter:Iterator[((String,String),Int)])={
    var conn:Connection=null
    var ps:PreparedStatement=null
    val sql="insert into iplocation(longitude,latitude,total_count)values(?,?,?)"
    conn=DriverManager.getConnection("jdbc:mysql://192.168.25.141:3306/spark","root","123456")
    ps = conn.prepareStatement(sql)
    iter.foreach(line=>{
      ps.setString(1,line._1._1)
      ps.setString(2,line._1._1)
      ps.setInt(3,line._2)
      ps.execute()
    })
  }

  def main(args: Array[String]): Unit = {
    val sparkConf:SparkConf = new SparkConf().setAppName("IpLocation").setMaster("local[5]")
    val sc = new SparkContext(sparkConf)
    val city_ip_rdd:RDD[(String, String, String, String)] = sc.textFile("D:\\bigdata\\data\\ip.txt").map(_.split("\\|")).map(x => (x(2), x(3), x(13), x(14)))
    //val unit1 = city_ip_rdd.foreach(x=>println(x._1,x._2,x._3,x._4))
    //println(unit1)
    val cityIpBroadcast:Broadcast[Array[(String,String,String,String)]] = sc.broadcast(city_ip_rdd.collect)
    val destData:RDD[String] = sc.textFile("D:\\bigdata\\data\\20090121000132.394251.http.format").map(_.split("\\|")(1))
  //  destData.foreach(x=>println(x))

    val result: RDD[((String, String), Int)] = destData.mapPartitions(iter => {
      val broadcastValue: Array[(String, String, String, String)] = cityIpBroadcast.value
      iter.map(ip => {
        val ipNum: Long = ip2Long(ip)
        val index: Int = binarySearch(ipNum, broadcastValue)
        ((broadcastValue(index)._3, broadcastValue(index)._4), 1)

      })
    })
    val unit2 = result.foreach(x=>println(x._1._1,x._1._2,x._2))
  //  println(unit2)
    val finalResult:RDD[((String,String),Int)] = result.reduceByKey(_+_)
    finalResult.collect().foreach(x=>println(x))
//    finalResult.foreachPartition(data2mysql)
    sc.stop()

  }

}

