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
public class UnionTestJava {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("test");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("Error");
        JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("a", "b", "c", "d", "e"),2);
        JavaRDD<String> rdd2 = sc.parallelize(Arrays.asList("a", "b", "f", "g", "h"),3);
        JavaRDD<String> result = rdd1.union(rdd2);
        System.out.println("rdd1 partiton length = "+rdd1.getNumPartitions());
        System.out.println("rdd2 partiton length = "+rdd2.getNumPartitions());
        System.out.println("result partiton length = "+result.getNumPartitions());
        result.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

    }
}
