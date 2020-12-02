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
 *  SparkSQL行列变化：
 *     +--------+----+-----+
 *     |username|item|price|          +--------+----+----+---+----+
 *     +--------+----+-----+          |username|   A|   B|  C|   D|
 *     |zhangsan|   A|    1|          +--------+----+----+---+----+
 *     |zhangsan|   B|    2|          |  wangwu|null|null|  8|null|
 *     |zhangsan|   C|    3|          |zhangsan|   1|   2|  3|   6|
 *     |    lisi|   A|    4|          |    lisi|   4|   7|  5|null|
 *     |    lisi|   C|    5|          +--------+----+----+---+----+
 *     |zhangsan|   D|    6|
 *     |    lisi|   B|    7|
 *     |  wangwu|   C|    8|
 *     +--------+----+-----+
 *    SQL函数：
 *     str_to_map(字段，分隔符1，分隔符2) ： 把当前字符串字段按照分隔符1切分成多条数据，再对每条数据按照分隔符2切割成K,V格式的数据组成Map
 *     map(K1,V1,K2,V2,K3,V3... ...) : 得到一个map集合
 */
object RowColumnTransfer2 {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("test").master("local").getOrCreate()
    session.sparkContext.setLogLevel("Error")
    val frame = session.read.option("header",true).csv("./data/rowcolumnData")
    frame.createTempView("temp1")
    session.sql(
      """
        |  select
        |     username,concat_ws("#",collect_list(concat(item,",",price))) as cw
        |  from temp1
        |  group by username
      """.stripMargin).createTempView("temp2")

    /**
     * +--------+---------------+
     * |username|             cw|
     * +--------+---------------+
     * |  wangwu|            C,8|
     * |zhangsan|A,1#B,2#C,3#D,6|
     * |    lisi|    A,4#C,5#B,7|
     * +--------+---------------+
     */

    session.sql(
      """
        | select
        |   username,str_to_map(cw,"#",",") as mp
        | from temp2
      """.stripMargin).createTempView("temp3")

    session.sql(
      """
        | select username ,mp['A'] as A,mp['B'] as B ,mp['C'] as C ,mp['D'] as D
        | from temp3
      """.stripMargin).createTempView("temp4")

    /**
     * +--------+--------------------+
     * |username|                  mp|
     * +--------+--------------------+
     * |  wangwu|            [C -> 8]|
     * |zhangsan|[A -> 1, B -> 2, ...|
     * |    lisi|[A -> 4, C -> 5, ...|
     * +--------+--------------------+
     */

    session.sql(
      """
        | select username,item,price
        | from
        | (select
        |   username,explode(map("A",A,"B",B,"C",C,"D",D)) as (item,price)
        | from temp4) ttt
        | where price is not null
      """.stripMargin).show(100,false)

    /**
     * +--------+----+----+---+----+
     * |username|   A|   B|  C|   D|
     * +--------+----+----+---+----+
     * |  wangwu|null|null|  8|null|
     * |zhangsan|   1|   2|  3|   6|
     * |    lisi|   4|   7|  5|null|
     * +--------+----+----+---+----+
     */
    session.sql(
      """
        | select * from temp4
        |""".stripMargin
    ).show()
  }
}
