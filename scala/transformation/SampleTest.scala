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
object SampleTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)
    val lines : RDD[String] = sc.textFile("./data/words")
    //sampe(有无放回抽样，抽样比例,种子)
    val result: RDD[String] = lines.sample(false,0.1,100L)
    result.foreach(println)


  }
}
