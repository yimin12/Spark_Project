package sparksql

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/12/1 0:48
*   @Description : 
*
*/
object ReadJsonRDDToDF {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().master("local").appName("test").getOrCreate()

    val jsonArr :Array[String] = Array[String](
      "{\"name\":\"zhangsan\",\"age\":18}",
      "{\"name\":\"lisi\",\"age\":19}",
      "{\"name\":\"wangwu\",\"age\":20}",
      "{\"name\":\"maliu\",\"age\":21}",
      "{\"name\":\"tianqi\",\"age\":22}"
    )
    import session.implicits._
    val jsonDataset: Dataset[String] = jsonArr.toList.toDS()

    val df1: DataFrame = session.read.json(jsonDataset)
    df1.createTempView("t")
    val df2: DataFrame = session.sql("select name,age from t where name like '%zhangsan%'")
    df2.show()
  }
}
