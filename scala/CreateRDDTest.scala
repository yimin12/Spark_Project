import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/11/30 13:45
*   @Description : 
*
*/
object CreateRDDTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("test")
    val sc = new SparkContext(conf)
    val result: RDD[(String, Int)] = sc.makeRDD(Array[(String, Int)](
      ("a", 10),
      ("b", 20),
      ("c", 30)
    ))
    result.foreach(println)

  }
}
