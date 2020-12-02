package sparksql.examples

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/12/1 21:56
*   @Description : 
*
*/
/**
 *  需求：
 *     读取 vpnlog 日志文件，其中userName为用户名，ts为记录时间，当type为login时，为登入时间，type为logout为登出时间
 *     问题：
 *     1）这一天，每个小时在线用户数
 *     2）计算这一天，各个用户的在线总时长，在线次数，最大在线时长
 *     （如果用户一天开始是登出记录，则认为他零点登入，如果一天结束时登入日志，则认为他24点登出）
 *     要求使用SparkSQL实现，并给出计算逻辑说明
 *
 *     SQL函数：
 *       replace(列，字符串1，字符串2) ：对某列的数据查找字符串1替换成字符串2
 */
object VpnLog {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().master("local").appName("test").getOrCreate()
    session.sparkContext.setLogLevel("Error")
    //读取json格式的数据
    val df = session.read.json("./data/vpnlog")
    df.createTempView("temp1")

    //对时间进行转换
    session.sql(
      """
        | select
        |   username,replace(replace(ts,"T"," "),".000Z","") as ts,type
        | from temp1
      """.stripMargin).createTempView("temp2")

    //给表中的数据打标号
    session.sql(
      """
        | select
        |     username,ts,type,row_number() over(partition by username order by ts ) as rank
        | from temp2
      """.stripMargin).createTempView("temp3")

    //进行自关联，错位匹配
    session.sql(
      """
        | select a.username as username1 ,a.ts as ts1,a.type as type1,a.rank as rank1,
        |        b.username as username2 ,b.ts as ts2,b.type as type2,b.rank as rank2
        | from temp3 a full outer join temp3 b on a.username = b.username and a.rank = b.rank-1
        | order by a.username,a.ts
      """.stripMargin).createTempView("temp4")

    //数据填补
    session.sql(
      """
        | select
        |   username1 ,ts1,type1,username2,ts2,type2
        | from
        | (select
        |   case when username1 is null then username2 else username1 end username1 ,
        |   case when ts1 is null then concat(split(ts2," ")[0],' 00:00:00') else ts1 end ts1,
        |   case when type1 is null then 'login' else type1 end type1,
        |   case when username2 is null then username1 else username2 end username2,
        |   case when ts2 is null then concat(split(ts1," ")[0],' 23:59:59') else ts2 end ts2,
        |   case when type2 is null then 'logout' else type2 end type2
        | from temp4) t
        | where type1 = 'login' and type2 = 'logout'
      """.stripMargin).createTempView("temp5")

    session.udf.register("myudf",(t:String,count:Int)=>{
      val list = new ListBuffer[String]()
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val calendar: Calendar = Calendar.getInstance()
      calendar.setTime(sdf.parse(t))
      for(i <- 0 to count){
        list.append(sdf.format(calendar.getTime))
        calendar.add(Calendar.HOUR,1)
      }
      list
    })

    //对数据进行一对多转换
    session.sql(
      """
        | select  username1 as username,ts1,type1,username2,ts2,type2,explode(myudf(ts1,hour(ts2)-hour(ts1))) as tt
        | from temp5
      """.stripMargin).createTempView("temp6")

    //对时间进行转换
    session.sql(
      """
        |  select distinct username, from_unixtime(unix_timestamp(tt,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH') as transtime
        |  from temp6
        |  order by username,transtime
      """.stripMargin).createTempView("temp7")

    //统计每个小时段在线的用户数
    session.sql(
      """
        | select  transtime,count(username) as usercount
        | from temp7
        | group by transtime
        | order by transtime
      """.stripMargin).show(100)

    //统计每次访问的时长 ， 分钟为单位
    session.sql(
      """
        | select
        |   username1 as username ,ts1,ts2,
        |   cast (((hour(ts2)*60*60+minute(ts2)*60+second(ts2)*1) - (hour(ts1)*60*60+minute(ts1)*60+second(ts1)*1))/60 as int) as dur
        | from temp5
      """.stripMargin).createTempView("temp8")

    //统计各个指标
    session.sql(
      """
        | select
        |    username,sum(dur) as totaldur,count(*) as totalcount,max(dur) as maxdur
        | from temp8
        | group by username
      """.stripMargin).show(100)

  }
}
