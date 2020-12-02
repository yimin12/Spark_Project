package sparksql

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/12/1 15:25
*   @Description : 
*
*/
class MyUDAF extends UserDefinedAggregateFunction{
  //调用UDAF函数时，传参的类型
  override def inputSchema: StructType = StructType(List[StructField](
    StructField("xx", DataTypes.StringType)
  ))

  //设置 在计算过程中，更新的数据类型
  override def bufferSchema: StructType = StructType(List[StructField](
    StructField("xx",DataTypes.IntegerType)
  ))

  //指定调用函数最后返回数据类型
  override def dataType: DataType = DataTypes.IntegerType

  //多次运行，结果顺序保持一致
  override def deterministic: Boolean = true

  // 作用在map和reduce两侧，给每个分区内的每个分组的数据做初始值
  override def initialize(buffer: MutableAggregationBuffer): Unit = buffer.update(0,0)
  //作用在map端每个分区内的每个分组上
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = buffer.update(0,buffer.getInt(0)+1)
  //作用在reduce端，每个分区的每个分组上，对map的结果做聚合
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = buffer1.update(0,buffer1.getInt(0)+buffer2.getInt(0))

  //调用函数最后返回的数据结果
  override def evaluate(buffer: Row): Any = buffer.getInt(0)
}

object SparkSQLUDAF {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().master("local").appName("test").getOrCreate()
    val nameList = List[String]("zhangsan","lisi","zhangsan","zhangsan","zhangsan","lisi","wangwu","wangwu","lisi","maliu")
    import session.implicits._
    val frame = nameList.toDF("name")
    frame.createTempView("infos")
    /**
     * 可以自己定义聚合函数实现 多行数据对应一个结果的功能。例如：自定义UDAF函数实现一个count功能
     */
    session.udf.register("namecount",new MyUDAF())
    session.udf.register("namecount",new UserDefinedAggregateFunction {
      //调用UDAF函数时，传参的类型
      override def inputSchema: StructType = StructType(List[StructField](
        StructField("xx",DataTypes.StringType)
      ))

      //设置 在计算过程中，更新的数据类型
      override def bufferSchema: StructType = StructType(List[StructField](
        StructField("xx",DataTypes.IntegerType)
      ))

      //指定调用函数最后返回数据类型
      override def dataType: DataType = DataTypes.IntegerType

      //多次运行，结果顺序保持一致
      override def deterministic: Boolean = true

      // 作用在map和reduce两侧，给每个分区内的每个分组的数据做初始值
      override def initialize(buffer: MutableAggregationBuffer): Unit = buffer.update(0,0)
      //作用在map端每个分区内的每个分组上
      override def update(buffer: MutableAggregationBuffer, input: Row): Unit = buffer.update(0,buffer.getInt(0)+1)
      //作用在reduce端，每个分区的每个分组上，对map的结果做聚合
      override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = buffer1.update(0,buffer1.getInt(0)+buffer2.getInt(0))

      //调用函数最后返回的数据结果
      override def evaluate(buffer: Row): Any = buffer.getInt(0)
    })
    session.sql(
      """
        | select name,namecount(name) as totalCount from infos group by name
      """.stripMargin).show()
  }
}
