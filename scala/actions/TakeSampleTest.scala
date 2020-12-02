package actions

import org.apache.spark.{SparkConf, SparkContext}

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/11/30 13:45
*   @Description : 
*
*/
object TakeSampleTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(Array[String]("a","b","c","d","e","f","g"))
    val result = rdd1.takeSample(false,3,100L)
    result.foreach(println)

  }
}
