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
object ZipAndZipWithIndexTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)


    val rdd1 = sc.parallelize(Array[String]("a","b","c","d","e"))
    val rdd2 = sc.parallelize(Array[Int](1,2,3,4,5))

    /**
     * zipWithIndex
     */
    val result: RDD[(String, Long)] = rdd1.zipWithIndex()
    result.foreach(println)
    //    val result: RDD[(String, Int)] = rdd1.zip(rdd2)
    //    result.foreach(println)
  }
}
