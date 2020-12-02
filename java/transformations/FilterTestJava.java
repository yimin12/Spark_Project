package transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

/*
 *   @Author : Yimin Huang
 *   @Contact : hymlaucs@gmail.com
 *   @Date : 2020/11/29 23:00
 *   @Description :
 *
 */
public class FilterTestJava {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("filter test");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("./data/words");
        JavaRDD<String> filter = lines.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String line) throws Exception {
                return "hello spark".equals(line);
            }
        });
        filter.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println("line = "+s);
            }
        });
    }
}
