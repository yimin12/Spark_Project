package persists;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/*
 *   @Author : Yimin Huang
 *   @Contact : hymlaucs@gmail.com
 *   @Date : 2020/11/29 22:24
 *   @Description :
 *
 */
public class CheckPointTestJava {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("cache test");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setCheckpointDir("./data/ck");
        JavaRDD<String> lines = sc.textFile("./data/words");
        lines.cache();
        lines.checkpoint();
        lines.count();
    }
}
