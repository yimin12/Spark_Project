package transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/11/30 13:45
*   @Description : 
*
*/
object GroupByKeyTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(Array[(String, Int)](
      ("zhangsan", 10),
      ("zhangsan", 20),
      ("lisi", 30),
      ("wangwu", 40),
      ("wangwu", 50)
    ))
    val result: RDD[(String, Iterable[Int])] = rdd.groupByKey()
    result.foreach(println)


  }
}
