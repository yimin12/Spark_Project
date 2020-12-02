import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.util.LongAccumulator;

import java.util.List;

/*
 *   @Author : Yimin Huang
 *   @Contact : hymlaucs@gmail.com
 *   @Date : 2020/11/29 16:19
 *   @Description :
 *
 */
public class AccumulatorTestJava {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("test");
        JavaSparkContext sc = new JavaSparkContext(conf);
        LongAccumulator longAccumulator = sc.sc().longAccumulator();
        JavaRDD<String> lines = sc.textFile("./data/words",10);
        JavaRDD<String> result = lines.map(new Function<String, String>() {
            @Override
            public String call(String line) throws Exception {
                longAccumulator.add(1L);
                return line;
            }
        });
        List<String> collect = result.collect();
        System.out.println("Accumulator's value：" + longAccumulator.value());

    }
}
