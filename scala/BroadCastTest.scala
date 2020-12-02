import org.apache.spark.{SparkConf, SparkContext}

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/11/30 13:38
*   @Description : 
*
*/
object BroadCastTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("test")
    val sc = new SparkContext(conf)
    val blackList = List[String]("zhangsan","maliu")
    val bcNameList = sc.broadcast(blackList)
    val nameInfos = sc.textFile("./data/nameInfos")
    nameInfos.filter(name=>{
      val valueList = bcNameList.value
      valueList.contains(name)
    }).foreach(println)
  }

}
