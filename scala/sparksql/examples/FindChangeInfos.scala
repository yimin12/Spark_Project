package sparksql.examples

import org.apache.spark.sql.SparkSession

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/12/1 17:04
*   @Description : 
*
*/
/**
 * 案例： 找出变化的行
 *   开窗函数 + 表的自关联实现
 */
object FindChangeInfos {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().master("local").appName("test").getOrCreate()
    session.sparkContext.setLogLevel("Error")
    val frame = session.read.option("header",true).csv("./data/test.csv")
    frame.createTempView("temp")
    session.sql(
      """
        |select id,change,name,row_number() over(partition by id order by name ) as rank
        |from temp
      """.stripMargin).createTempView("t")

    session.sql(
      """
        |select id,change,name,row_number() over(partition by id order by name ) as rank
        |from temp
      """.stripMargin).show()

    session.sql(
      """
        | select a.id,a.change,a.name
        | from t a join t b on a.id = b.id
        | where a.change != b.change and a.rank = b.rank-1
      """.stripMargin).show()
  }
}
