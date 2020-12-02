package actions;

import org.apache.spark.SparkConf;
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
 *      Calculate how many times of specific key happens
 */
public class CountByKeyTestJava {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("test");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(Arrays.asList(
                new Tuple2<String, Integer>("zhangsan", 1),
                new Tuple2<String, Integer>("zhangsan", 2),
                new Tuple2<String, Integer>("lisi", 3),
                new Tuple2<String, Integer>("wangwu", 4),
                new Tuple2<String, Integer>("maliu", 5),
                new Tuple2<String, Integer>("zhangsan", 6)
        ));
        Map<String, Long> map = rdd.countByKey();
        Set<Map.Entry<String, Long>> entries = map.entrySet();
        for(Map.Entry<String, Long> entry : entries){
            String key = entry.getKey();
            Long value = entry.getValue();
            System.out.println("key = "+key +",value = "+value);
        }
    }
}
