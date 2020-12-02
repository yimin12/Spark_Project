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
 *   @Date : 2020/11/29 22:30
 *   @Description :
 *
 */
public class CoalesceTestJava {

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
                List<String> list = new ArrayList<>();
                while(iter.hasNext()){
                    String next = iter.next();
                    list.add("rdd1 partition index = [ " + index + " ] and value is " + next);
                }
                return list.iterator();
            }
        }, false);
        System.out.println("rdd2's partition number is " + rdd2.getNumPartitions());
        /**
         * coalesce : 可以对RDD进行重分区，可以增多分区，也可以减少分区，默认没有shuffle。
         *     注意： coalesce默认不产生shuffle，当由少的分区重分区到多的分区时，不起作用。如果想要起作用，需要指定产生shuffle
         *  repartition(num) = coalesce(num,shuffle = true)
         */
        JavaRDD<String> coalesce = rdd2.coalesce(2,true);
        System.out.println("coalesce partition length = "+coalesce.getNumPartitions());

        JavaRDD<String> result = coalesce.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer index, Iterator<String> iter) throws Exception {
                List<String> list = new ArrayList<>();
                while (iter.hasNext()) {
                    String one = iter.next();
                    list.add("coalesce RDD partition index = " + index + ",value = " + one);

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
