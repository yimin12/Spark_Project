import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/*
 *   @Author : Yimin Huang
 *   @Contact : hymlaucs@gmail.com
 *   @Date : 2020/11/29 21:21
 *   @Description :
 *
 */
public class SparkWordCountJava {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("spark_workcount_java");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("./data/words");
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        JavaPairRDD<String, Integer> pairRDD = words.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairRDD<String, Integer> result = pairRDD.reduceByKey((v1, v2)-> v1 + v2); // only manipulate in value position
        result.foreach(tuple2 -> System.out.println(tuple2));

    }
}
