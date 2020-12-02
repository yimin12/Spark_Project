package transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;

/*
 *   @Author : Yimin Huang
 *   @Contact : hymlaucs@gmail.com
 *   @Date : 2020/11/29 21:26
 *   @Description :
 *
 */
public class InsertectionTestJava {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("test");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("Error");
        JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("a", "b", "c", "d", "e"),2);
        JavaRDD<String> rdd2 = sc.parallelize(Arrays.asList("a", "b", "f", "g", "h"),3);
        JavaRDD<String> result = rdd1.intersection(rdd2);
        System.out.println("rdd1 partition lenght = "+rdd1.getNumPartitions());
        System.out.println("rdd2 partition lenght = "+rdd2.getNumPartitions());
        System.out.println("result partition lenght = "+result.getNumPartitions());

        // the number of final partitions will take the larger number
        result.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

    }
}
