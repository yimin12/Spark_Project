package sparksql

import org.apache.spark.sql.{SaveMode, SparkSession}

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/11/30 13:45
*   @Description : 
*
*/
object ReadHiveTableToDFLocal {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().master("local").appName("test").config("hive.metastore.uris","thrift://mynode1:9083")
      .enableHiveSupport().getOrCreate()
    session.sql("use spark")

    //创建 students 表
    session.sql(
      """
        | create table students(id int ,name string,age int) row format delimited fields terminated by ','
      """.stripMargin)
    //创建 score 表
    session.sql(
      """
        | create table scores(id int ,name string,score int) row format delimited fields terminated by ','
      """.stripMargin)
    //给students 表加载数据
    session.sql(
      """
        | load data local inpath './data/students' into table students
      """.stripMargin)
    //给scores表加载数据
    session.sql(
      """
        | load data local inpath './data/scores' into table scores
      """.stripMargin)

    //读取Hive表的数据，进行统计分析
    val result = session.sql(
      """
        | select a.id,a.name,a.age,b.score from students a join scores b on a.id = b.id where b.score >= 200
      """.stripMargin)
    result.show()

    //将结果数据保存在Hive表中
    result.write.mode(SaveMode.Overwrite).saveAsTable("goodinfos")
  }
}
