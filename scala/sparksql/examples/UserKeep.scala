package sparksql.examples

import org.apache.spark.sql.SparkSession

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/12/1 21:34
*   @Description : 
*
*/

/**
 * 根据用户的注册信息表及用户每日登录平台的数据信息来分析用户7日留存情况。
 *  SQL函数：
 *   datediff(日期1，日期2) : 计算日期1与日期2的相差天数，日期格式必须是 “yyyy-MM-dd”格式。
 *   unix_timestamp(日期,"yyyyMMdd") : 按照指定的格式将日期数据转换成时间戳。
 *   from_unixtime(timestamp,"yyyy-MM-dd") : 将时间戳转换成指定日期格式。
 *
 */
object UserKeep {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().master("local").appName("test").getOrCreate()
    session.sparkContext.setLogLevel("Error")
    val registerInfos = session.read.option("header",true).csv("./data/registuser.csv")
    val loginInfos = session.read.option("header",true).csv("./data/loginInfos.csv")

    registerInfos.createTempView("regist")
    loginInfos.createTempView("login")

    //对login 表中的数据 相同用户相同日期登录的数据进行去重
    session.sql(
      """
        |select distinct uid,login_date from login
      """.stripMargin).createOrReplaceTempView("login")

    //用户注册信息表与用户登录信息表进行关联，找出每个用户登录日期与注册日期的差值
    session.sql(
      """
        | select
        |   b.uid,b.regist_day,a.login_date,
        |   datediff(from_unixtime(unix_timestamp(a.login_date,'yyyyMMdd'),'yyyy-MM-dd'),
        |     from_unixtime(unix_timestamp(b.regist_day,'yyyyMMdd'),'yyyy-MM-dd')) as diff
        | from login a join regist b
        | on a.uid = b.uid
      """.stripMargin).createTempView("temp")

    //统计注册日期的 7 日留存情况
    session.sql(
      """
        | select
        |   regist_day ,diff ,count(*) as usercount
        | from temp
        | where diff != 0
        | group by regist_day ,diff
        | order by regist_day,diff
      """.stripMargin).show()
  }
}
