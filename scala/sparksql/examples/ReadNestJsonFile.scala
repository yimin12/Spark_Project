package sparksql.examples

import org.apache.spark.sql.SparkSession

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/12/1 18:34
*   @Description : 
*
*/
object ReadNestJsonFile {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().master("local").appName("test").getOrCreate()
    val df = session.read.json("./data/NestJsonFile")
    df.printSchema()
    df.createTempView("t")
    session.sql(
      """
        | select name,score,infos.gender,infos.age from t
        """.stripMargin
    ).show()

    import org.apache.spark.sql.functions._
    df.select(col("name"), col("score"),col("infos.gender"),col("infos.age")).show(100, false)
  }
}
