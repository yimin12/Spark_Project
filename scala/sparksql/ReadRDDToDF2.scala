package sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/12/1 14:31
*   @Description : 
*
*/
/**
 * 通过动态创建Schema方式将普通格式的RDD转换成DataFrame
 *   注意：
 *     创建StructType类型的数据时，StructField字段的顺序需要与构建的RDD[Row]中每个Row中放入数据的顺序保持一致。
 */
object ReadRDDToDF2 {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().master("local").appName("test").getOrCreate()
    val sc = session.sparkContext
    sc.setLogLevel("Error")
    val personInfos:RDD[String] = sc.textFile("./data/personInfo")
    val rowRDD: RDD[Row] = personInfos.map(line => {
      val arr = line.split(",")
      val id = arr(0).toInt
      val name = arr(1)
      val age = arr(2).toInt
      val score = arr(3).toDouble
      Row(id, name, age, score)
    })
    val structType = StructType(List[StructField](
      StructField("id",DataTypes.IntegerType,true),
      StructField("name",DataTypes.StringType,true),
      StructField("age",DataTypes.IntegerType,true),
      StructField("score",DataTypes.DoubleType,true)
    ))
    val frame = session.createDataFrame(rowRDD,structType)
    frame.show()
    frame.printSchema()
    frame.createTempView("t")
    session.sql(
      """
        | select id,name ,age ,score from t where id = 3
      """.stripMargin).show()
  }
}
