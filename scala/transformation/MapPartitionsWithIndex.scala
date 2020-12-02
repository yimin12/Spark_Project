package transformation

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/11/30 13:45
*   @Description : 
*
*/
object MapPartitionsWithIndex {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array[String]("a","b","c","d","e","f"),3)
    val result = rdd1.mapPartitionsWithIndex((index, iter) => {
      val listBuffer = new ListBuffer[String]()
      while (iter.hasNext) {
        val one = iter.next()
        listBuffer.append(s"partition : $index ,value : $one")
      }
      listBuffer.iterator
    })
    result.foreach(println)
  }
}
