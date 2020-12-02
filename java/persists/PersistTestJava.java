package persists;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

/*
 *   @Author : Yimin Huang
 *   @Contact : hymlaucs@gmail.com
 *   @Date : 2020/11/29 21:26
 *   @Description :
 *
 */
public class PersistTestJava {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("cache test");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("./data/persistData.txt");
        lines.persist(StorageLevel.MEMORY_ONLY());
        // mark the checkpoint

        long startTime1 = System.currentTimeMillis();
        long count1 = lines.count();
        long endTime1 = System.currentTimeMillis();
        System.out.println("磁盘:count1 = "+count1 +",time = "+(endTime1 - startTime1)+"ms");


        long startTime2 = System.currentTimeMillis();
        long count2 = lines.count();
        long endTime2 = System.currentTimeMillis();
        System.out.println("内存:count2 = "+count2 +",time = "+(endTime2 - startTime2)+"ms");
    }
}
