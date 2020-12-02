package sparksql

import org.apache.spark.sql.{SaveMode, SparkSession}

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/12/1 14:14
*   @Description : 
*
*/
object ReadParquetFileToDF {

  /**
   * 读取Parquet格式的数据加载DataFrame
   *   注意：
   *     1).parquet是一种列式存储格式，默认是有压缩。Spark中常用的一种数据格式
   *     2).读取Parquet格式的数据两种方式
   *     3).可以将DataFrame保存成Json或者Pauquet格式数据,注意保存模式
   */

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().master("local").appName("test").getOrCreate()
    session.sparkContext.setLogLevel("Error")
    val frame = session.read.json("./data/jsondata")
    /**
     * SaveMode:
     *   Append: 追加写数据
     *   ErrorIfExists : 存在就报错
     *   Ignore : 忽略
     *   Overwrite : 覆盖写数据
     */
    frame.write.mode(SaveMode.Overwrite).parquet("./data/parquet")
    val df = session.read.format("parquet").load("./data/parquet")
    df.write.json("./data/resultjson")
    df.show()
    val count: Long = df.count()
    println(s"total count = $count")
  }
}
