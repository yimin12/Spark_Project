package transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/*
 *   @Author : Yimin Huang
 *   @Contact : hymlaucs@gmail.com
 *   @Date : 2020/11/29 21:26
 *   @Description :
 *
 */
public class MapPartitionsWithIndexTestJava {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("test");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("Error");
        JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h"), 3);
        /**
         * mapPartitionsWithIndex :
         *  遍历RDD中的每个分区，同时可以获取RDD中数据的每个分区号。
         */
        JavaRDD<String> result = rdd1.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer index, Iterator<String> iter) throws Exception {
                List<String> list = new ArrayList<String>();
                while (iter.hasNext()) {
                    String next = iter.next();
                    list.add("partition : " + index + "\t value :" + next);
                }

                return list.iterator();
            }
        }, false);

        List<String> collect = result.collect();
        for(String s:collect){
            System.out.println(s);

        }
    }
}
