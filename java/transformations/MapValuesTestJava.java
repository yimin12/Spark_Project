package transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

/*
 *   @Author : Yimin Huang
 *   @Contact : hymlaucs@gmail.com
 *   @Date : 2020/11/29 21:26
 *   @Description :
 *
 */
public class MapValuesTestJava {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("test");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("Error");
        JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(Arrays.asList(
                new Tuple2<String, Integer>("zhangsan", 10),
                new Tuple2<String, Integer>("lisi", 20),
                new Tuple2<String, Integer>("wangwu", 30),
                new Tuple2<String, Integer>("maliu", 40),
                new Tuple2<String, Integer>("tianqi", 50),
                new Tuple2<String, Integer>("zhaoba", 60)
        ));
        JavaPairRDD<String, String> result = rdd1.mapValues(new Function<Integer, String>() {
            @Override
            public String call(Integer integer) throws Exception {
                return integer + " hello";
            }
        });
        result.foreach(new VoidFunction<Tuple2<String, String>>() {
            @Override
            public void call(Tuple2<String, String> stringStringTuple2) throws Exception {
                System.out.println(stringStringTuple2);
            }
        });
    }
}
