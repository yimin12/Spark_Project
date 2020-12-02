package actions

import org.apache.spark.{SparkConf, SparkContext}

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/11/30 13:45
*   @Description : 
*
*/
object TopAndTakeOrderedTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(Array[Int](1,2,3,4,5,6,7,8,9))
//    val result : Array[Int] = rdd1.takeOrdered(4)
//    result.foreach(println)
        val result: Array[Int] = rdd1.top(4)
        result.foreach(println)

  }
}
