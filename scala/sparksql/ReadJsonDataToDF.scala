package sparksql

import org.apache.spark.sql.{DataFrame, SparkSession}

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/11/30 13:45
*   @Description : 
*
*/
object ReadJsonDataToDF {
  def main(args: Array[String]): Unit = {
    val session :SparkSession = SparkSession.builder().master("local").appName("sparksqltest").getOrCreate()
    session.sparkContext.setLogLevel("Error")
    val df1: DataFrame = session.read.json("./data/jsondata")

    df1.createTempView("t1")
    df1.createGlobalTempView("t2")
    df1.createOrReplaceGlobalTempView("t")
    session.sql("select name ,age from global_temp.t2").show()
    println("----------------------------------------")
    val session2 :SparkSession = session.newSession()
    session2.sql("select name, age from global_temp.t2").show()
    println("----------------------------------------")
    session.sql("select name ,age+10 as addage from t").show()
    println("----------------------------------------")
    session.sql("select name ,sum(age) as totalage from t group by name").show()

    val frame: DataFrame = session.sql("select name, age from t")
    frame.show()

    //select name,age from table
    val df2: DataFrame = df1.select("name", "age")
    //df2.show(100)
    //select name,age from table where age is not null
//        val df2: DataFrame = df1.filter("age is not null")
//        val df3: DataFrame = df2.select("name","age")
//        df3.show()
    //select name ,age from table where age >= 19
    //    import org.apache.spark.sql.functions._
    //    df1.filter("age >= 19 and age is not null").select(col("name"),col("age")).show()
    //select name ,age + 10 as addage from table where age is not null
    //    df1.select(col("name"),col("age").plus(10).alias("addage"))
    //      .filter("age is not null")
    //      .show()


    //    frame.show()
    //    frame.printSchema()
  }
}
