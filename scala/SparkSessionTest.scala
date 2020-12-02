import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/11/30 14:38
*   @Description : 
*
*/
object SparkSessionTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("test")
    conf.setMaster("local")
    val sc1 = new SparkContext(conf)


    val session: SparkSession = SparkSession.builder().master("local").appName("test").getOrCreate()
    val sc2: SparkContext = session.sparkContext
  }
}
