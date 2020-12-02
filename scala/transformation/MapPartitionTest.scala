package transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/11/30 13:45
*   @Description : 
*
*/
object MapPartitionTest {
  /**
   * 建立数据库连接... ...
   * 批量插入数据库.. ...ListBuffer(b#, c#)
   * 关闭数据库连接... ...
   * 建立数据库连接... ...
   * 批量插入数据库.. ...ListBuffer(d#, e#)
   * 关闭数据库连接... ...
   * 建立数据库连接... ...
   * 批量插入数据库.. ...ListBuffer(a#)
   * 关闭数据库连接... ...
   *
   * @param args
   */

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(List[String]("a","b","c","d","e"),3)
    val result: RDD[String] = rdd.mapPartitions(iter => {
      val listBuffer = new ListBuffer[String]()
      println("建立数据库连接... ...")
      while (iter.hasNext) {
        val next = iter.next()
        listBuffer.append(next + "#")
      }
      println("批量插入数据库.. ..." + listBuffer.toString())
      println("关闭数据库连接... ...")
      listBuffer.iterator
    })
    val strings: Array[String] = result.collect()
    strings.foreach(println)

  }
}
