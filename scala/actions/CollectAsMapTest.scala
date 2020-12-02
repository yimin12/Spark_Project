package actions

import org.apache.spark.{SparkConf, SparkContext}

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/11/30 15:03
*   @Description : 
*
*/
object CollectAsMapTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(Array[(String, Int)](
      ("zhangsan", 18),
      ("lisi", 20),
      ("wangwu", 21),
      ("maliu", 22)
    ))
    val map: collection.Map[String, Int] = rdd.collectAsMap()
    map.foreach(println)
  }
}
