import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;
import java.util.List;

/*
 *   @Author : Yimin Huang
 *   @Contact : hymlaucs@gmail.com
 *   @Date : 2020/11/29 16:51
 *   @Description :
 *
 */
public class BoardCastTestJava {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("test");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("Error");
        List<String> blackList = Arrays.asList("zhangsan", "maliu");//Driver
        Broadcast<List<String>> bcBlackList = sc.broadcast(blackList);
        JavaRDD<String> nameInfos = sc.textFile("./data/nameInfos",10);

        /**
         * Handle it in the driver side
         */
//        nameInfos.filter(new Function<String, Boolean>() {
//            @Override
//            public Boolean call(String name) throws Exception {
//                List<String> executorList = bcBlackList.value();
//                return !executorList.contains(name);
//            }
//        }).foreach(new VoidFunction<String>() {
//            @Override
//            public void call(String s) throws Exception {
//                System.out.println(s);
//            }
//        });

        /**
         * Handle it in the executor side
         */
        nameInfos.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return !blackList.contains(s); //Executor端
            }
        }).foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });;
    }
}
