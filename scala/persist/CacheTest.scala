package persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/11/30 20:39
*   @Description : 
*
*/
/**
 * cache : 将RDD数据持久化到内存中，懒执行算子，需要Action算子触发执行。
 */
object CacheTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("cache test")
    val sc = new SparkContext(conf)
    var lines: RDD[String] = sc.textFile("./data/persistData.txt")
    //    lines = lines.cache()
    lines.cache()

    val startTime1 = System.currentTimeMillis()
    val count1: Long = lines.count()
    val endTime1 = System.currentTimeMillis()
    println(s"从磁盘读取数据：count1 = $count1,time = ${endTime1-startTime1}ms")

    val startTime2 = System.currentTimeMillis()
    val count2: Long = lines.count()
    val endTime2 = System.currentTimeMillis()
    println(s"从内存读取数据：count2 = $count2,time = ${endTime2-startTime2}ms")


  }
}
