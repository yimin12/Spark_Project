package transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
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
public class JoinTestJava {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("test");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("Error");
        JavaPairRDD<String, Integer> nameRDD = sc.parallelizePairs(Arrays.asList(
                new Tuple2<String, Integer>("zhangsan", 18),
                new Tuple2<String, Integer>("lisi", 19),
                new Tuple2<String, Integer>("wangwu", 20),
                new Tuple2<String, Integer>("maliu", 21)
        ));
        JavaPairRDD<String, Integer> scoreRDD = sc.parallelizePairs(Arrays.asList(
                new Tuple2<String, Integer>("zhangsan", 100),
                new Tuple2<String, Integer>("lisi", 200),
                new Tuple2<String, Integer>("wangwu", 300),
                new Tuple2<String, Integer>("tianqi", 400)
        ));
        /**
         * (zhangsan,(18,100))
         * (wangwu,(20,300))
         * (lisi,(19,200))
         */
        JavaPairRDD<String, Tuple2<Integer, Integer>> join = nameRDD.join(scoreRDD);
        join.foreach(new VoidFunction<Tuple2<String, Tuple2<Integer, Integer>>>() {
            @Override
            public void call(Tuple2<String, Tuple2<Integer, Integer>> stringTuple2Tuple2) throws Exception {
                System.out.println(stringTuple2Tuple2);
            }
        });
        sc.stop();
    }
}
