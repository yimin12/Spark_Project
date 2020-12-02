package actions

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/11/30 15:42
*   @Description : 
*
*/
object ForeachPartitionTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array[String]("a","b","c","d"),2)
    rdd1.foreachPartition(iter => {
      val list = new ListBuffer[String]()
      println("创建数据库连接")
      while(iter.hasNext){
        val next = iter.next()
        list.append(next)
      }
      println("插入数据库连接"+list.toString())
      println("关闭数据库连接")
    })
  }
}
