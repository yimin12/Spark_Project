package sparksql

import org.apache.spark.sql.SparkSession

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/12/1 15:12
*   @Description : 
*
*/
/**
 * 在本地操作Hive 使用开窗函数
 */
object SparkSQLOverFunOnHive {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().master("local").config("hive.metastore.uris","thrift://mynode1:9083").appName("test").enableHiveSupport().getOrCreate()
    session.sql("use spark")
    session.sql(
      """
        | create table sales(date string, category string, score double) row format delimited fields terminated by '\t'
        """.stripMargin
    )
    session.sql(
      """
        | load data local inpath './data/sales' into table sales
        """.stripMargin
    )

    val result = session.sql(
      """
        | select date, category, score, row_number() over (partition by category order by score desc) from sales
        """.stripMargin
    )
    result.write.saveAsTable("salesresult")
  }
}
