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
object RepartitionTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array[String](
      "love1", "love2", "love3", "love4", "love5", "love6", "love7", "love8", "love9", "love10", "love11", "love12"
    ), 3)
    val rdd2 = rdd1.mapPartitionsWithIndex((index,iter)=>{
      val listBuffer = new ListBuffer[String]()
      while(iter.hasNext){
        val one = iter.next()
        listBuffer.append(s"rdd1 partition index = $index ,value = $one")
      }
      listBuffer.iterator
    })
    val arr1 = rdd2.collect()
    arr1.foreach(println)

    println("--------------------------------------")

    val repartition = rdd2.repartition(2)
    val result = repartition.mapPartitionsWithIndex((index,iter)=>{
      val listBuffer = new ListBuffer[String]()
      while(iter.hasNext){
        val one = iter.next()
        listBuffer.append(s"repartition partition index = $index ,value = $one")
      }
      listBuffer.iterator
    })
    val arr = result.collect()
    arr.foreach(println)

  }
}
