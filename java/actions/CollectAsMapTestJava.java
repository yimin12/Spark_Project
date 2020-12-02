package actions;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

/*
 *   @Author : Yimin Huang
 *   @Contact : hymlaucs@gmail.com
 *   @Date : 2020/11/29 21:26
 *   @Description :
 *      Can be used to collect the latest value of specific key
 */
public class CollectAsMapTestJava {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("test");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(Arrays.asList(
                new Tuple2<String, Integer>("zhangsan", 18),
                new Tuple2<String, Integer>("lisi", 19),
                new Tuple2<String, Integer>("wangwu", 20),
                new Tuple2<String, Integer>("maliu", 21),
                new Tuple2<String, Integer>("zhangsan", 22)
        ));
        /**
         * collectAsMap : 针对K,V格式的RDD进行操作，将K,V格式的RDD回收到Driver端形成一个Map
         */
        Map<String, Integer> map = rdd1.collectAsMap();
        Set<Map.Entry<String, Integer>> entries = map.entrySet();
        for(Map.Entry<String, Integer> entry : entries){
            String key = entry.getKey();
            Integer value = entry.getValue();
            System.out.println("key = "+key +",value = "+value);
        }

    }
}
