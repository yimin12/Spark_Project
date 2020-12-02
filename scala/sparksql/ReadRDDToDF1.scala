package sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/12/1 14:21
*   @Description : 
*
*/


/**
 * 通过反射方式将普通格式的RDD转换成DataFrame
 *   注意: 反射的方式将自定义类型的RDD转换成DataFrame过程中，会自动将对象中的属性当做DataFrame 中的列名，将自定义对象中的属性的类型当做DataFrame
 *     列的schema信息。
 */
case class PersonInfo(id:Int, name:String, age:Int, score: Double)

object ReadRDDToDF1 {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().master("local").appName("test").getOrCreate()
    val sc = session.sparkContext
    sc.setLogLevel("Error")
    val personInfos: RDD[String] = sc.textFile("./data/personInfo")
    val personRDD: RDD[PersonInfo] = personInfos.map(info =>{
      val arr = info.split(",")
      val id = arr(0).toInt
      val name = arr(1)
      val age = arr(2).toInt
      val score = arr(3).toDouble
      PersonInfo(id, name, age, score)
    })
    import session.implicits._
    val frame: DataFrame = personRDD.toDF()
    frame.printSchema()
    frame.show()

    frame.createTempView("t")
    val result: DataFrame = session.sql(
      """
        | select id, name, age, score from t
        """.stripMargin
    )
    result.show()
  }
}
