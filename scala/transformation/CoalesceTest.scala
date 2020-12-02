package transformation

import org.apache.spark.{SparkConf, SparkContext}

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/11/30 17:51
*   @Description : 
*
*/
object CoalesceTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array[String](
      "love1", "love2", "love3", "love4", "love5", "love6", "love7", "love8", "love9", "love10", "love11", "love12"
    ), 3)
    val result = rdd1.coalesce(4,true)
    println(s"result partition length = "+result.getNumPartitions)
    result.foreach(println)
  }
}
