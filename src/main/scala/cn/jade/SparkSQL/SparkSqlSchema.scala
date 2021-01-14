package cn.jade.SparkSQL

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


object SparkSqlSchema {
  def main(args: Array[String]): Unit = {
    val sparkSession:SparkSession = SparkSession.builder()
      .appName("SparkSqlSchema ")
      .master("local[2]")
      .getOrCreate()
    val sc:SparkContext = sparkSession.sparkContext
    val dataRDD:RDD[String] = sc.textFile("E:\\bigdata\\data\\person.txt")
    val dataArrayRDD:RDD[Array[String]] = dataRDD.map(_.split(" "))


    val personRDD:RDD[Row] = dataArrayRDD.map(x=>Row(x(0).toInt,x(1),x(2).toInt))
    val schema:StructType = StructType(Seq(
      StructField("id",IntegerType,false),
      StructField("name",StringType,false),
      StructField("age",IntegerType,false)
    ))
    val personDF:DataFrame = sparkSession.createDataFrame(personRDD,schema)


    personDF.show()
    personDF.createOrReplaceTempView("t_person")
    sparkSession.sql("select * from t_person").show()
    sparkSession.sql("select count(*) from t_person").show()
    sc.stop()
    sparkSession.stop()



  }

}
