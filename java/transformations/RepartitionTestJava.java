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
public class RepartitionTestJava {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("test");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("Error");
        JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList(
                "love1", "love2", "love3", "love4",
                "love5", "love6", "love7", "love8",
                "love9", "love10", "love11", "love12"
        ), 3);

        JavaRDD<String> rdd2 = rdd1.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer index, Iterator<String> iter) throws Exception {
                List<String> list = new ArrayList<String>();
                while(iter.hasNext()){
                    String next = iter.next();
                    list.add("rdd1 partition index = 【"+index+"】\tvalue=【"+next+"】");

                }
                return list.iterator();
            }
        }, false);
        /**
         * repartition  :可以对RDD重新分区，可以增多分区，也可以减少分区。会产生shuffle。
         */
//        JavaRDD<String> repartition = rdd2.repartition(4);
        JavaRDD<String> repartition = rdd2.repartition(2);
        JavaRDD<String> result = repartition.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer index, Iterator<String> iter) throws Exception {
                List<String> list = new ArrayList<>();
                while (iter.hasNext()) {
                    String next = iter.next();
                    list.add("repartition partition index = " + index + ",value = " + next);
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
