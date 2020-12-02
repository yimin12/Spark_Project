import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;

/*
 *   @Author : Yimin Huang
 *   @Contact : hymlaucs@gmail.com
 *   @Date : 2020/11/29 20:58
 *   @Description :
 *
 */
public class SelfDefineAccumulatorTest {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("test");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("Error");
        SelfAccumulator acc = new SelfAccumulator();
        sc.sc().register(acc);
        JavaRDD<String> infos = sc.parallelize(Arrays.asList("A 1", "B 2", "C 3", "D 4", "E 5", "F 6", "G 7", "H 8", "I 9"), 3);
        infos.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
        infos.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                Integer age = Integer.valueOf(s.split(" ")[1]);
                acc.add(new PersonInfo(1, age));
                return s;
            }
        }).count();
        PersonInfo value = acc.value();
        System.out.println("acc value is" + value);
    }
}
