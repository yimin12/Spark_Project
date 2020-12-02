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
object DistinctTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array[String]("a","b","c","d","d","a","b","a","c","d"))

    /**
     * 需求：对以上数据进行去重 ： a  b c d
     */
    val result: RDD[String] = rdd1.distinct()
    result.foreach(println)


  }
}
