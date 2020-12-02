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
object FilterTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)
    val lines: RDD[String] = sc.textFile("./data/words")
    val result: RDD[String] = lines.filter(line => {
      !"hello spark".equals(line)
    })
    result.foreach(println)

  }
}
