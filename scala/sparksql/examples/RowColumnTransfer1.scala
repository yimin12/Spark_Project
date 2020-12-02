package sparksql.examples

import org.apache.spark.sql.SparkSession

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/12/1 18:43
*   @Description : 
*
*/
/**
 * 数据的行列变化：
 *   多行数据到一行数据中
 *    例如：
 *     +--------+----+-----+           +--------+---------+-----+
 *     |username|item|price|           |username|item     |price|
 *     +--------+----+-----+           +--------+---------+-----+
 *     |zhangsan|   A|    1|           |zhangsan|A,B,C,D |   12|
 *     |zhangsan|   B|    2|           |lisi    |A,B,C   |   16|
 *     |zhangsan|   C|    3|           |wangwu | C       |    8|
 *     |    lisi|   A|    4|           +--------+--------+-----+
 *     |    lisi|   C|    5|
 *     |zhangsan|   D|    6|
 *     |    lisi|   B|    7|
 *     |  wangwu|   C|    8|
 *     +--------+----+-----+
 *     collect_list(item) : 将item数据根据分组合并到一个集合中。
 *     collect_set(item)  : 将item数据根据分组合并到一个集合中,相同的数据会去重。
 *     concat(xx,xxx... ...) : 拼接字符串
 *     concat_ws (分隔符，集合):按照分隔符将集合中的元素进行拼接，返回字符串
 *     split(列，"分隔符") ： 对某列进行按照分隔符切割得到对应的一个集合
 *     explode(集合) ： 一边多转换数据
 *
 */
object RowColumnTransfer1 {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().master("local").appName("test").getOrCreate()
    val df = session.read.option("header",true).csv("./data/rowcolumnData")
    df.createTempView("temp1")

    session.sql(
      """
        | select
        |   username,collect_list(item) as cl ,sum(price) as totalprice
        | from temp1
        | group by  username
      """.stripMargin).createTempView("temp2")

    session.sql(
      """
        | select
        |   username,concat_ws(",",cl) as cw,totalprice
        | from temp2
      """.stripMargin).createTempView("temp3")

    session.sql(
      """
        | select * from temp3
        |""".stripMargin
    ).show()

//    session.sql(
//      """
//        | select
//        |   username,explode(split(cw,",")) as item ,totalprice
//        | from temp3
//      """.stripMargin).show(100)

  }
}
