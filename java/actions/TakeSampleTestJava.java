package actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/*
 *   @Author : Yimin Huang
 *   @Contact : hymlaucs@gmail.com
 *   @Date : 2020/11/29 21:26
 *   @Description :
 *
 */
public class TakeSampleTestJava {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("test");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("Error");
        JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("a", "b", "c", "d", "e", "f", "g"));

        /**
         * Randomly pick up from data set
         */
        List<String> result = rdd1.takeSample(false, 3, 101L); // false: 没有放回， 3: 三个抽样 100L: 随机池是 100L
        for(String s : result){
            System.out.println(s);
        }
     }
}
