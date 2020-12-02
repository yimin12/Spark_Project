package sparksql

import java.util.Properties

import org.apache.hadoop.hdfs.server.namenode.SafeMode
import org.apache.spark.sql.{DataFrame, DataFrameReader, SaveMode, SparkSession}

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/12/1 13:54
*   @Description : 
*
*/
object ReadMySQLDataToDF {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().config("spark.sql.shuffle.partitions",1).master("local").appName("test").getOrCreate()
    session.sparkContext.setLogLevel("Error")
    // the first method
    val props = new Properties()
    props.setProperty("user","root")
    props.setProperty("password","123456")
    val person: DataFrame = session.read.jdbc("jdbc:mysql://192.168.119.14/spark", "person", props)
    person.createTempView("person")

    // the second method
    val map = Map[String, String](
      "user"->"root",
      "password"->"123456",
      "url"->"jdbc:mysql://192.168.179.14/spark",
      "driver"->"com.mysql.jdbc.Driver",
      "dbtable"->"score"
    )
    val score = session.read.format("jdbc").options(map).load()
    score.createTempView("score")

    val resultDF = session.sql(
      """
        |select a.id, a.name, a.age, b.score from person a, score b where a.id = b.id
        """.stripMargin
    )
    resultDF.write.mode(SaveMode.Append).jdbc("jdbc:mysql://192.168.119.10/spark", "result", props)

    // the third method
    val reader: DataFrameReader = session.read.format("jdbc").option("user", "root")
      .option("password", "123456")
      .option("url", "jdbc:mysql://192.168.179.14/spark")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "score")
    val score2 = reader.load()
    score2.show()
  }
}
