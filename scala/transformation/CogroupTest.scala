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
object CogroupTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array[(String,Int)](
      ("zhangsan",1),
      ("lisi",2),
      ("zhangsan",3),
      ("wangwu",4),
      ("wangwu",5)
    ))
    val rdd2 = sc.parallelize(Array[(String,Int)](
      ("zhangsan",10),
      ("lisi",20),
      ("zhangsan",30),
      ("wangwu",40),
      ("wangwu",50)
    ))

    val result: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2);
    result.foreach(println)
//    result.foreach(tp =>{
//      val key = tp._1;
//      val value: Iterable[Int] = tp._2._1
//      value.foreach(println)
//      tp._2._2.foreach(println)
//    })

  }
}
