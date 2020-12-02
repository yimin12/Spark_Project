package sparksql.examples

import org.apache.spark.sql.SparkSession

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/12/1 17:40
*   @Description : 
*
*/
object ReadJsonArrayData {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("test").master("local").getOrCreate()
    val frame = session.read.json("./data/jsonArrayFile")
    import session.implicits._
    import org.apache.spark.sql.functions._

    val df1 = frame.select(frame.col("name"), frame.col("age"), explode(frame.col("scores")).as("el"))
    df1.select($"name",col("age"),col("el.xueqi"),col("el.yuwen"),
      col("el.shuxue"),col("el.yingyu")).show()

    frame.createTempView("temp")
    val df = session.sql(
      """
        | select
        |   name,age ,el.xueqi,el.yuwen,el.shuxue,el.yingyu
        | from
        | (select name,age,explode(scores) as el  from temp ) t
      """.stripMargin).show()


  }
}
