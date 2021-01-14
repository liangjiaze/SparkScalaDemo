package cn.jade.wordCount;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class WordCount_Java {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("WordCount_java").setMaster("local[2]");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        JavaRDD <String> dataJavaRDD = jsc.textFile("E:\\\\words.txt");

        JavaRDD <String> wordsJavaRDD = dataJavaRDD.flatMap(new FlatMapFunction <String, String>() {
            public Iterator <String> call(String line) throws Exception {
                String[] words = line.split(" ");
                return Arrays.asList(words).iterator();
            }
        });

        JavaPairRDD <String, Integer> wordAndOneJavaPairRDD = wordsJavaRDD.mapToPair(new PairFunction <String, String, Integer>() {
            public Tuple2 <String, Integer> call(String word) throws Exception {
                return new Tuple2 <String, Integer>(word, 1);
            }
        });

        JavaPairRDD <String, Integer> resultJavaPairRDD = wordAndOneJavaPairRDD.reduceByKey(new Function2 <Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        JavaPairRDD <Integer, String> reverseJavaPairRDD = resultJavaPairRDD.mapToPair(new PairFunction <Tuple2 <String, Integer>, Integer, String>() {
            public Tuple2 <Integer, String> call(Tuple2 <String, Integer> tuple2) throws Exception {
                return new Tuple2 <Integer, String>(tuple2._2, tuple2._1);
            }
        });

        JavaPairRDD <String, Integer> sortJavaPairRDD = reverseJavaPairRDD.sortByKey(false).mapToPair(new PairFunction <Tuple2 <Integer, String>, String, Integer>() {
            public Tuple2 <String, Integer> call(Tuple2 <Integer, String> tuple2) throws Exception {
                return new Tuple2 <String, Integer>(tuple2._2, tuple2._1);
            }
        });

        List <Tuple2 <String, Integer>> finalResult = sortJavaPairRDD.collect();

        for (Tuple2 <String, Integer> tuple : finalResult) {
            System.out.println(tuple._1+ "出现的次数" +tuple._2);
        }

        jsc.stop();

    }
}
