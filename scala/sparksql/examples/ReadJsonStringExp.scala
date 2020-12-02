package sparksql.examples

import org.apache.spark.sql.SparkSession

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/12/1 18:30
*   @Description : 
*
*/


/**
 *  读取json格式的字符串直接解析出json中属性对应的值： get_json_object(jsonstr,json属性)
 */
object ReadJsonStringExp {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().master("local").appName("test").getOrCreate()
    val jsonList = List[String](
      "{\"name\":\"zhangsan\",\"age\":18}",
      "{\"name\":\"lisi\",\"age\":19}",
      "{\"name\":\"wangwu\",\"age\":20}",
      "{\"name\":\"maliu\",\"age\":21}"
    )
    import session.implicits._
    import org.apache.spark.sql.functions._
    val df = jsonList.toDF("info")

    df.createTempView("t")
    session.sql(
      """
        | select info, get_json_object(info,"$.name") as name, get_json_object(info, "$.age") as age from t
        """.stripMargin
    ).show(false)
  }
}
