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
object MapValueTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(Array[(String, Int)](
      ("zhangsan", 100),
      ("lisi", 100),
      ("wangwu", 300),
      ("maliu", 400),
      ("tianqi", 500)
    ))
    val end: RDD[(String, Int)] = rdd1.mapValues(value =>{value+100})
    end.foreach(println)

  }
}
