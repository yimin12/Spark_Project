package example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/*
 *   @Author : Yimin Huang
 *   @Contact : hymlaucs@gmail.com
 *   @Date : 2020/11/30 17:43
 *   @Description :
 *
 */
public class SecondSortTestJava {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("test");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("Error");
        JavaRDD<String> lines = sc.textFile("./data/SecondSort.txt");
        JavaPairRDD<SecondSort, String> pairInfo = lines.mapToPair(new PairFunction<String, SecondSort, String>() {
            @Override
            public Tuple2<SecondSort, String> call(String s) throws Exception {
                String first = s.split("\t")[0];
                String second = s.split("\t")[1];
                return new Tuple2<>(new SecondSort(Integer.valueOf(first), Integer.valueOf(second)), s);
            }
        });
        pairInfo.sortByKey(false).foreach(new VoidFunction<Tuple2<SecondSort, String>>() {
            @Override
            public void call(Tuple2<SecondSort, String> tp) throws Exception {
                System.out.println(tp._2);
            }
        });
    }
}
