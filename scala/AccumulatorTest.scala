import org.apache.spark.{SparkConf, SparkContext}

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/11/29 16:06
*   @Description : 
*
*/
object AccumulatorTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("test")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    val acc = sc.longAccumulator
    //    var i = 0
    val lines = sc.textFile("./data/words",2)
    val result = lines.map(one => {
      acc.add(1L)
      one
    })
    val strings = result.collect()

    println(s"acc value = ${acc.value}")



    //    val result = lines.map(one => {
    //      i += 1
    //      println("Executor i = "+i)
    //      one
    //    })
    //    val array = result.collect()
    //    println(s" i = $i")

  }
}
