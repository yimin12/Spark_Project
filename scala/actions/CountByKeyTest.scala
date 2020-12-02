package actions

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/11/30 15:23
*   @Description : 
*
*/
object CountByKeyTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)
    val rdd :RDD[(String, Int)] = sc.parallelize(Array[(String, Int)](
      ("zhangsan", 1),
      ("lisi", 2),
      ("wangwu", 3),
      ("lisi", 4),
      ("zhangsan", 5)
    ))
    val map: collection.Map[String, Long] = rdd.countByKey();
    map.foreach(println)
  }
}
