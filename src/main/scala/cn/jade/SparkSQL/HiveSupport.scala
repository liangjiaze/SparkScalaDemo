package cn.jade.SparkSQL

import org.apache.spark.sql.SparkSession

object HiveSupport {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("HiveSupport")
      .master("local[2]")
      .config("spark.sql.warehouse.dir", "E:\\bigdata\\data\\spark-warehouse")
      .enableHiveSupport()
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel("WARN")
    // 2、操作sql语句

//    sparkSession.sql("CREATE TABLE IF NOT EXISTS person (id int, name string, age int) row format delimited fields terminated by ','")
//    sparkSession.sql("LOAD DATA LOCAL INPATH './data/student.txt' INTO TABLE person")
    sparkSession.sql("select * from person ").show()
    sparkSession.stop()


  }

}
