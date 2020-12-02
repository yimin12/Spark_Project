package sparksql

import org.apache.spark.sql.SparkSession

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/11/30 23:35
*   @Description : 
*     Read the csv data from source
*/
object ReadCsvDataToDF {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().master("local").appName("test").getOrCreate()
    val frame = session.read.option("header", true).format("csv").load("./data/csvdata.csv")
    frame.show()
    frame.write.csv("./xxx")
  }
}
