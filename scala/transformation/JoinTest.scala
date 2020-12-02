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
object JoinTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    val nameRDD: RDD[(String, Int)] = sc.parallelize(Array[(String, Int)](
      ("zhangsan", 18),
      ("lisi", 19),
      ("wangwu", 20),
      ("maliu", 21)
    ),3)
    val scoreRDD: RDD[(String, Int)] = sc.parallelize(Array[(String, Int)](
      ("zhangsan", 100),
      ("lisi", 200),
      ("wangwu", 300),
      ("tianqi", 400)
    ),4)

    val result: RDD[(String, (Int, Int))] = nameRDD.join(scoreRDD)
    println(s"nameRDD partition length = ${nameRDD.getNumPartitions}")
    println(s"scoreRDD partition length = ${scoreRDD.getNumPartitions}")
    println(s"result partition length = ${result.getNumPartitions}")
    result.foreach(println)
    sc.stop()


  }
}
