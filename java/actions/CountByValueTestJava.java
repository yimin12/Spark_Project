package actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.codehaus.janino.Java;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

/*
 *   @Author : Yimin Huang
 *   @Contact : hymlaucs@gmail.com
 *   @Date : 2020/11/29 21:42
 *   @Description :
 *
 */
public class CountByValueTestJava {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("test");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("Error");
        JavaRDD<String> rdd = sc.parallelize(Arrays.asList("a", "b", "c", "d", "e", "a", "f", "c", "a"),3);
        Map<String, Long> map = rdd.countByValue();
        Set<Map.Entry<String, Long>> entries = map.entrySet();
        for(Map.Entry<String, Long> entry : entries){
            String key = entry.getKey();
            Long value = entry.getValue();
            System.out.println("key = "+key +",value = "+value);
        }

        JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(Arrays.asList(
            new Tuple2<String, Integer>("zhangsan", 1),
            new Tuple2<String, Integer>("zhangsan", 2),
            new Tuple2<String, Integer>("lisi", 1),
            new Tuple2<String, Integer>("wangwu", 3),
            new Tuple2<String, Integer>("maliu", 2),
            new Tuple2<String, Integer>("zhangsan", 1)
        ));

        Map<Tuple2<String, Integer>, Long> map1 = rdd1.countByValue();
        Set<Map.Entry<Tuple2<String, Integer>, Long>> entries1 = map1.entrySet();
        for(Map.Entry<Tuple2<String, Integer>, Long> entry : entries1){
            Tuple2<String, Integer> key = entry.getKey();
            Long val = entry.getValue();
            System.out.println("Key : " + key + ", value : " + val);
        }
    }
}
