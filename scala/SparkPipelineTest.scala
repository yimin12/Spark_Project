import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/11/30 13:45
*   @Description : 
*
*/
object SparkPipelineTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("test")
    val sc = new SparkContext(conf)
    val rdd1 :RDD[String] = sc.textFile("./data/words")

    val rdd2 = rdd1.map(line =>{
      println(s"**** map 操作 **** $line")
      line + "#"
    })

    val rdd3 = rdd2.filter(line => {
      println(s"**** filter 操作 **** $line")
      true
    })
    rdd3.count()

    val lines :RDD[String] = sc.textFile("./data/words")
    println(s"lines RDD partition 个数 = ${lines.getNumPartitions}")
    val words: RDD[String] = lines.flatMap(line=>{line.split(" ")})
    val pairWords: RDD[(String, Int)] = words.map(word=>{new Tuple2(word,1)})
    val reduceRDD: RDD[(String, Int)] = pairWords.reduceByKey((v1, v2)=>{v1+v2},4)
    println(s"reduceRDD partition 个数 = ${reduceRDD.getNumPartitions}")

    val result: RDD[(String, Int)] = reduceRDD.sortBy(tp=>{tp._2},false)
    result.count()
    sc.stop()
  }
}
