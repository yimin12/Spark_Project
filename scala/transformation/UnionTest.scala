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
object UnionTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)
    val rdd1 = sc.makeRDD(Array[String]("a","b","c","d"))
    val rdd2 = sc.parallelize(Array[String]("a","b","e","f"))
    val result: RDD[String] = rdd1.union(rdd2)
    result.foreach(println)


  }
}
