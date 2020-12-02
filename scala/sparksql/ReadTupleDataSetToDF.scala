package sparksql

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/12/1 14:35
*   @Description : 
*
*/
/**
 *  Tuple格式的DataSet加载DataFrame
 */
object ReadTupleDataSetToDF {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().master("local").appName("test").getOrCreate()
    session.sparkContext.setLogLevel("Error")
    import session.implicits._
    val ds: Dataset[String] = session.read.textFile("./data/pvuvdata")
    val tupleDs: Dataset[(String, String, String, String, String, String, String)] = ds.map(line => {
      //126.54.121.136	浙江	2020-07-13	1594648118250	4218643484448902621	www.jd.com	Comment
      val arr = line.split("\t")
      val ip = arr(0)
      val local = arr(1)
      val date = arr(2)
      val ts = arr(3)
      val uid = arr(4)
      val site = arr(5)
      val operator = arr(6)
      (ip, local, date, ts, uid, site, operator)
    })
    val frame: DataFrame = tupleDs.toDF("ip","local","date","ts","uid","site","operator")
    frame.createTempView("t")
    //pv
    session.sql(
      """
        |  select site ,count(*) as pv from t group by site order by pv
      """.stripMargin).show()
    //uv
    session.sql(
      """
        | select site,count(*) uv from (select distinct ip,site from t) t1 group by site order by uv
      """.stripMargin).show()
  }
}
