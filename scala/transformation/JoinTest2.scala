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
object JoinTest2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    val nameRDD: RDD[(String, Int)] = sc.parallelize(Array[(String, Int)](
      ("zhangsan", 18),
      ("lisi", 19),
      ("wangwu", 20),
      ("maliu", 21)
    ))
    val scoreRDD: RDD[(String, Int)] = sc.parallelize(Array[(String, Int)](
      ("zhangsan", 100),
      ("lisi", 200),
      ("wangwu", 300),
      ("tianqi", 400)
    ))

    /**
     * fullOuterJoin
     */
    val result: RDD[(String, (Option[Int], Option[Int]))] = nameRDD.fullOuterJoin(scoreRDD)
    result.foreach(println)

    /**
     * rightOuterJoin
     */
    //    val result: RDD[(String, (Option[Int], Int))] = nameRDD.rightOuterJoin(scoreRDD)
    //    result.foreach(println)
    /**
     * leftOuterJoin
     */
    //    val result: RDD[(String, (Int, Option[Int]))] = nameRDD.leftOuterJoin(scoreRDD)
    //    result.foreach(tp=>{
    //      val key = tp._1
    //      val value1 = tp._2._1
    //      val value2 = tp._2._2.getOrElse("null")
    //      println(s"key = $key ,value1 = $value1,value2 = $value2")
    //    })



  }
}
