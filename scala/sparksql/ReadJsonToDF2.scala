package sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/12/1 0:42
*   @Description : 
*
*/
object ReadJsonToDF2 {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder().master("local").appName("readjson").getOrCreate()
    session.sparkContext.setLogLevel("Error")
    val df1: DataFrame = session.read.format("json").load("./data/jsondata")
    df1.createTempView("t")
    val df2: DataFrame = session.sql("select name,age +10 as addage from t ")
    val rdd: RDD[Row] = df2.rdd
    rdd.foreach(row=>{
      val name = row.getAs[String]("name")
      val addage = row.getAs[Long]("addage")
      println(s"name is $name,age is $addage")
    })

  }
}
