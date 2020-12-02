package transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

/*
 *   @Author : Yimin Huang
 *   @Contact : hymlaucs@gmail.com
 *   @Date : 2020/11/29 23:15
 *   @Description :
 *
 */
public class JoinTestJava2 {

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
         * fullOuterJoin 作用在K,V格式RDD上 （K,V）.fullOuterJoin(K,W) => (K,(Optional[V],Optional[W]))
         */
        JavaPairRDD<String, Tuple2<Optional<Integer>, Optional<Integer>>> result = nameRDD.fullOuterJoin(scoreRDD);
        result.foreach(new VoidFunction<Tuple2<String, Tuple2<Optional<Integer>, Optional<Integer>>>>() {
            @Override
            public void call(Tuple2<String, Tuple2<Optional<Integer>, Optional<Integer>>> tp) throws Exception {
                System.out.println(tp);
            }
        });

        /**
         * rightOuterJoin
         */
        JavaPairRDD<String, Tuple2<Optional<Integer>, Integer>> result2 = nameRDD.rightOuterJoin(scoreRDD);
        result2.foreach(new VoidFunction<Tuple2<String, Tuple2<Optional<Integer>, Integer>>>() {
            @Override
            public void call(Tuple2<String, Tuple2<Optional<Integer>, Integer>> tp) throws Exception {
                System.out.println(tp);
            }
        });

        /**
         * leftOuterJoin : 以左侧的RDD 出现的key为主 ，作用在K,V格式的RDD上
         *  （K,V）.leftOuterJoin(K,W) => (K,(V,Optional[W]))
         */
        JavaPairRDD<String, Tuple2<Integer, Optional<Integer>>> leftOuterJoin = nameRDD.leftOuterJoin(scoreRDD);
        leftOuterJoin.foreach(new VoidFunction<Tuple2<String, Tuple2<Integer, Optional<Integer>>>>() {
            @Override
            public void call(Tuple2<String, Tuple2<Integer, Optional<Integer>>> tp) throws Exception {
                String key = tp._1;
                Integer value1 = tp._2._1;
                Optional<Integer> value2 = tp._2._2;
                if(value2.isPresent()){

                    System.out.println("key = "+key+",value1 = "+value1+",value2 = "+value2.get());
                }else{
                    System.out.println("key = "+key+",value1 = "+value1+",value2 = NULL");

                }

            }
        });


    }
}
