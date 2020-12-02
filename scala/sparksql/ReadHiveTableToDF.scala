package sparksql

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/11/30 23:42
*   @Description : 
*
*/
object ReadHiveTableToDF {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("test").enableHiveSupport().getOrCreate()
    session.sql("use spark")
    val df: DataFrame = session.sql("select count(*) from jizhan")
    df.show()

    //读取Hive表中的数据加载DataFrame
    val df2 = session.table("jizhan")
    df2.show()

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
        | load data local inpath '/software/test/students' into table students
      """.stripMargin)
    //给scores表加载数据
    session.sql(
      """
        | load data local inpath '/software/test/scores' into table scores
      """.stripMargin)

    //读取Hive表的数据，进行统计分析
    val result = session.sql(
      """
        | select a.id,a.name,a.age,b.score from students a join scores b on a.id = b.id where b.score >= 200
      """.stripMargin)
    result.show()

    //将结果数据保存在Hive表中
    result.write.mode(SaveMode.Overwrite).saveAsTable("goodinfos")




    //    val conf = new SparkConf()
    //    conf.setAppName("test")
    //    val sc = new SparkContext(conf)
    //    val sqlContext = new SQLContext(sc)
    //    val hiveContext = new HiveContext(sc)
    //    hiveContext.sql("use spark")
    //    hiveContext.sql("select count(*) from jizhan").show()




  }
}
