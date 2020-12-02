package transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import parquet.it.unimi.dsi.fastutil.ints.IntIterable;
import scala.Tuple2;

import java.util.Arrays;

/*
 *   @Author : Yimin Huang
 *   @Contact : hymlaucs@gmail.com
 *   @Date : 2020/11/29 21:26
 *   @Description :
 *
 */
public class CoGroupTestJava {

    /**
     * cogroup: 作用在K,V格式的RDD上，(K,V).cogroup(K,W) => (K,([V],[W])),可以对比两个RDD 对应key下的value异同。
     *
     */

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("test");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("Error");
        JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(Arrays.asList(
                new Tuple2<String, Integer>("zhangsan", 18),
                new Tuple2<String, Integer>("zhangsan", 180),
                new Tuple2<String, Integer>("zhangsan", 1800),
                new Tuple2<String, Integer>("lisi", 20),
                new Tuple2<String, Integer>("lisi", 200),
                new Tuple2<String, Integer>("wangwu", 21),
                new Tuple2<String, Integer>("maliu", 22)
        ));
        JavaPairRDD<String, Integer> rdd2 = sc.parallelizePairs(Arrays.asList(
                new Tuple2<String, Integer>("zhangsan", 1),
                new Tuple2<String, Integer>("zhangsan", 2),
                new Tuple2<String, Integer>("zhangsan", 3),
                new Tuple2<String, Integer>("lisi", 4),
                new Tuple2<String, Integer>("wangwu", 5),
                new Tuple2<String, Integer>("wangwu", 6),
                new Tuple2<String, Integer>("tianqi", 7)
        ));

        JavaPairRDD<String, Tuple2<Iterable<Integer>, Iterable<Integer>>> result = rdd1.cogroup(rdd2);
        result.foreach(new VoidFunction<Tuple2<String, Tuple2<Iterable<Integer>, Iterable<Integer>>>>() {
            @Override
            public void call(Tuple2<String, Tuple2<Iterable<Integer>, Iterable<Integer>>> tp) throws Exception {
                System.out.println(tp);
            }
        });
    }
}
