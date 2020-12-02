package actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

/*
 *   @Author : Yimin Huang
 *   @Contact : hymlaucs@gmail.com
 *   @Date : 2020/11/29 21:26
 *   @Description :
 *
 */
public class ForeachTestJava {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("test");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("./data/words");

        List<String> collect = lines.collect();
        for(String s : collect){
            System.out.println(s);
        }

        String first = lines.first();
        System.out.println(first);
        List<String> take = lines.take(10);
        System.out.println("------------------------");
        for(String s : take){
            System.out.println(s);
        }

        long count = lines.count();
        System.out.println("count = " + count);
    }
}
