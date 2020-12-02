package sparksql

import org.apache.spark.sql.SparkSession

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/12/1 16:44
*   @Description : 
*
*/
object SparkSQLUDF {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().master("local").appName("test").getOrCreate()
    val nameList = List[String]("zhangsan","lisi","wangwu","maliu")
    import session.implicits._
    val frame = nameList.toDF("name")
    frame.createTempView("info")

    /**
     * 自定义 namelength 函数来计算每个name的长度。
     *
     */
    session.udf.register("NAMELENGTH",(n1:String, i:Int)=>{
      i + n1.length()
    })
    session.sql(
      """
        | select name,namelength(name,10) as nl from info order by nl desc
        """.stripMargin
    ).show()
  }
}
