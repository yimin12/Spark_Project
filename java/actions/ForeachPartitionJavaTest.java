package actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

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
public class ForeachPartitionJavaTest {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("test");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("a", "b", "c", "d", "e"), 2);

        /**
         * 将以上数据进行保存数据库
         * mapPartitions
         *
         * foreachPartition ： 针对RDD中分区的数据进行操作
         */
        rdd1.foreachPartition(new VoidFunction<Iterator<String>>() {
            @Override
            public void call(Iterator<String> stringIterator) throws Exception {
                List<String> list = new ArrayList<>();
                System.out.println("Connect the database");
                while(stringIterator.hasNext()){
                    String next = stringIterator.next();
                    list.add(next);
                }
                System.out.println(list.toString());
                System.out.println("Insert new data to database");
                System.out.println("Disconnect to database");
            }
        });
    }
}
