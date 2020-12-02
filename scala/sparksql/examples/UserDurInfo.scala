package sparksql.examples

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/12/1 21:08
*   @Description : 
*     data:  111	2019-06-20	1
*/
case class AccInfo(uid:String,accDate:String,dur:Int)
object UserDurInfo {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().master("local").appName("test").getOrCreate()

    val sc = session.sparkContext
    sc.setLogLevel("Error")
    val infos = sc.textFile("./data/userAccInfo.txt")
    val accInfoRDD: RDD[AccInfo] = infos.map(info => {
      val arr = info.split("\t")
      val uid = arr(0)
      val accDate = arr(1)
      val dur = arr(2).toInt
      AccInfo(uid, accDate, dur)
    })
    import session.implicits._
    val frame = accInfoRDD.toDF()
    frame.createTempView("accInfo")

    // add all dur with preceding value
    session.sql(
      """
        | select uid,accDate,sum(dur) over(partition by uid order by accDate ) as current_day_dur
        | from accInfo
      """.stripMargin).show()

    // add (cur + last) dur
    session.sql(
      """
        | select uid,accDate,sum(dur) over(partition by uid order by accDate rows between 1 preceding and current row ) as totalDur
        | from accInfo
      """.stripMargin).show()

    // add (last + cur + next) dur
    session.sql(
      """
        | select uid,accDate,sum(dur) over(partition by uid order by accDate rows between 1 preceding and 1 following) as totalDur
        | from accInfo
      """.stripMargin).show()

    session.sql(
      """
        | select uid,sum(dur) as totaldur
        | from accInfo
        | group by uid
      """.stripMargin).show()

    session.sql(
      """
        | select uid,accdate,sum(dur) over(partition by uid) as totaldur
        | from accInfo
      """.stripMargin).show()

  }

}
