package cn.jade.SparkSQL

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

case class Person(id:Int, name:String, age:Int)

object CaseClassSchema {
  def main(args: Array[String]): Unit = {
    val sparkSession:SparkSession = SparkSession.builder()
      .appName("CaseClassSchema ")
      .master("local[2]").getOrCreate()
    val sc:SparkContext = sparkSession.sparkContext
    sc.setLogLevel("WARN")
    val dataRDD:RDD[String] = sc.textFile("D:\\bigdata\\data\\person.txt")
    val lineArrayRDD:RDD[Array[String]] = dataRDD.map(_.split(" "))


    val personRDD:RDD[Person] = lineArrayRDD.map(x=>Person(x(0).toInt,x(1),x(2).toInt))
    import sparkSession.implicits._
    val personDF:DataFrame = personRDD.toDF()


    //todo-------------------DSL语法操作 start--------------
    personDF.show()
    personDF.printSchema()
    personDF.columns.foreach(println)

    println(personDF.count())
    println(personDF.head())
    personDF.select("name").show()
    personDF.filter($"age">30).show()
    println(personDF.filter($"age">30).count())
    personDF.groupBy("age").count().show()

    //todo--------------------SQL操作风格 start-----------
    personDF.createOrReplaceTempView("t_person")
    sparkSession.sql("select * from t_person").show()
    sparkSession.sql("select * from t_person order by age desc").show()
    sparkSession.sql("select * from t_person where name='zhangsan'").show()
    sc.stop()
    sparkSession.stop()





  }
}
