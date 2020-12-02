package transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

/*
 *   @Author : Yimin Huang
 *   @Contact : hymlaucs@gmail.com
 *   @Date : 2020/11/29 21:26
 *   @Description :
 *
 */
public class ZipAndZipWithIndexTestJava {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("test");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("Error");
        JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("a", "b", "c", "d", "e","f"),3);
        JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(100,200,300,400,500));
        /**
         * zipWithIndex : RDD中的数据和本条数所在的下标压缩在一起，组成K,V格式的RDD返回。
         */
        JavaPairRDD<String, Long> result = rdd1.zipWithIndex();
        result.foreach(new VoidFunction<Tuple2<String, Long>>() {
            @Override
            public void call(Tuple2<String, Long> tp) throws Exception {
                System.out.println(tp);
            }
        });
//        /**
//         * zip :作用在两个RDD 上，生成一个K,V格式的RDD,要求两个RDD中每个分区内的元素个数相同
//         */
//        JavaPairRDD<String, Integer> result2 = rdd1.zip(rdd2);
//        result2.foreach(new VoidFunction<Tuple2<String, Integer>>() {
//            @Override
//            public void call(Tuple2<String, Integer> tuple2) throws Exception {
//                System.out.println(tuple2);
//            }
//        });
    }
}
