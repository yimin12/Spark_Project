package transformation

import org.apache.spark.{SparkConf, SparkContext}

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/11/30 13:45
*   @Description : 
*
*/
object SubtractTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(List[Int](1,2,3,4,5,6))
    val rdd2 = sc.parallelize(List[Int](1,2,3,4,7,8))
    val result = rdd1.subtract(rdd2)
    result.foreach(println)
  }
}
