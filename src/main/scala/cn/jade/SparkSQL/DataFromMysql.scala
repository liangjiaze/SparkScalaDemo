package cn.jade.SparkSQL

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

object DataFromMysql {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("DataFromMysql ")
      .master("local[2]")
      .getOrCreate()
    val properties: Properties = new Properties()
    properties.setProperty("user","root")
    properties.setProperty("password","123456")
    val mysqlDF: DataFrame = spark.read.jdbc("jdbc:mysql://node1:3306/spark","iplocation",properties)
    mysqlDF.show()
    spark.stop()
  }

}
