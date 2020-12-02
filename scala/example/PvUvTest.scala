package example

import org.apache.spark.{SparkConf, SparkContext}

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/11/30 23:03
*   @Description : 
*
*/
object PvUvTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("test")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    val lines = sc.textFile("./data/pvuvdata")
    //126.54.121.136	浙江	2020-07-13	1594648118250	4218643484448902621	www.jd.com	Comment
    //126.54.121.136	浙江	2020-07-13	1594648118250	4218643484448902621	www.jd.com	Buy

    /**
     * pv
     */
    val pairSite = lines.map(line => {
      (line.split("\t")(5),1)
    })
    val reduceInfo = pairSite.reduceByKey((v1, v2) => {v1 + v2})
    val pv = reduceInfo.sortBy(tp=>{tp._2},false)
    pv.foreach(println)

    println("*******************************************")
    /**
     * uv:
     * 126.54.121.136	浙江	2020-07-13	1594648118250	4218643484448902621	www.jd.com	Comment
     * 126.54.121.136	浙江	2020-07-13	1594648118250	4218643484448902621	www.jd.com	Buy
     */
    lines.map(line => {
      line.split("\t")(0) + "_" + line.split("\t")(5)
    }).distinct().map(one=>{(one.split("_")(1),1)}).reduceByKey((v1,v2) => {v1 + v2}).sortBy(tp=>{tp._2},false).foreach(println)
  }
}
