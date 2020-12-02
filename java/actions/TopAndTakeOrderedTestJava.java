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
public class TopAndTakeOrderedTestJava {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("test");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("Error");
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(9,1, 2, 3, 4, 5, 6, 7, 8));
        /**
         * takeOrdered : 针对RDD 获取RDD中最小的前num个值，返回一个集合，放在Driver端。
         * top k small
         */
        List<Integer> result = rdd.takeOrdered(3);
        for(Integer i:result){
            System.out.println(i);

        }

        /**
         * top K large
         */
        System.out.println("-----------------------");
        List<Integer> res = rdd.top(4);
        for(Integer i : res){
            System.out.println(i);
        }
    }
}
