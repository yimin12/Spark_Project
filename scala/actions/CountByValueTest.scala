package actions

import org.apache.spark.{SparkConf, SparkContext}

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/11/30 15:32
*   @Description : 
*
*/
object CountByValueTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(Array[String]("a", "b", "c", "d", "e", "a", "f", "c", "a"))
    val map: collection.Map[String, Long] = rdd.countByValue();
    map.foreach(println)
  }
}
