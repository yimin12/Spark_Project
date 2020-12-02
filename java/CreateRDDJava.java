import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.codehaus.janino.Java;
import scala.Tuple2;
import scala.Tuple4;

import java.util.Arrays;

/*
 *   @Author : Yimin Huang
 *   @Contact : hymlaucs@gmail.com
 *   @Date : 2020/11/29 18:11
 *   @Description :
 *      Logic is similar with python
 *      It has tuple concept here
 */
public class CreateRDDJava {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("Create RDD");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Tuple2<String, Integer>> parallize = sc.parallelize(Arrays.asList(
                new Tuple2<String, Integer>("a", 10),
                new Tuple2<String, Integer>("b", 20),
                new Tuple2<String, Integer>("c", 30)
        ));

        JavaRDD<Tuple4<String, String, Integer, Integer>> FPairs = sc.parallelize(Arrays.asList(
                new Tuple4<>("a", "b", 10, 20),
                new Tuple4<>("c", "d", 30, 40)
        ));

        JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(Arrays.asList(
                new Tuple2<String, Integer>("a", 10),
                new Tuple2<String, Integer>("b", 20),
                new Tuple2<String, Integer>("c", 30)
        ));

        FPairs.foreach(new VoidFunction<Tuple4<String, String, Integer, Integer>>() {
            @Override
            public void call(Tuple4<String, String, Integer, Integer> stringStringIntegerIntegerTuple4) throws Exception {
                System.out.println(stringStringIntegerIntegerTuple4);
            }
        });

//        parallize.foreach(new VoidFunction<Tuple2<String, Integer>>() {
//            @Override
//            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
//                System.out.println(stringIntegerTuple2);
//            }
//        });
    }
}
