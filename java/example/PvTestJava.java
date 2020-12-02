package example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/*
 *   @Author : Yimin Huang
 *   @Contact : hymlaucs@gmail.com
 *   @Date : 2020/11/30 16:17
 *   @Description :
 *
 */
public class PvTestJava {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("test");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("Error");
        JavaRDD<String> lines = sc.textFile("./data/pvuvdata");
        //126.54.121.136	浙江	2020-07-13	1594648118250	4218643484448902621	www.jd.com	Comment
        //126.54.121.136	浙江	2020-07-13	1594648118250	4218643484448902621	www.jd.com	Buy
        /**
         * uv : unique vistor
         */
        JavaRDD<String> ipSiteIpInfo = lines.map(new Function<String, String>() {
            @Override
            public String call(String line) throws Exception {
                String ip = line.split("\t")[1]; // 126.54.121.136
                String site = line.split("\t")[5]; // www.jd.com
                return ip + "_" + site;
            }
        });
        JavaRDD<String> distinctIpSiteInfo = ipSiteIpInfo.distinct(); // deduplicate
        JavaPairRDD<String, Integer> siteInfo = distinctIpSiteInfo.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String site = s.split("_")[1];
                return new Tuple2<>(site, 1);
            }
        });
        JavaPairRDD<String, Integer> reduceInfo = siteInfo.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        // sorted by the frequency of key
        JavaPairRDD<String, Integer> uv = reduceInfo.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple2) throws Exception {
                return tuple2.swap();
            }
        }).sortByKey(false).mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                return integerStringTuple2.swap();
            }
        });
        uv.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2);
            }
        });

        System.out.println("--------------------------------------");
        /**
         * pv : page view
         */
        JavaPairRDD<String, Integer> pairSiteInfo = lines.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String site = s.split("\t")[5];
                return new Tuple2<>(site, 1);
            }
        });
        JavaPairRDD<String, Integer> reduceInfopv = pairSiteInfo.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        JavaPairRDD<String, Integer> pv = reduceInfopv.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.swap();
            }
        }).sortByKey(false).mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            // sortbyKey(true) : sorted in ascending order
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                return integerStringTuple2.swap();
            }
        });
        pv.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2);
            }
        });
    }
}
