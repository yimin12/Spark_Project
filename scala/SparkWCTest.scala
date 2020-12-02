import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/11/30 14:45
*   @Description : 
*
*/
object SparkWCTest {

  def main(args: Array[String]): Unit = {
    /**
     * SparkConf 是Spark的配置，可以设置：
     *   1).Spark运行模式
     *     local:本地运行模式，多用于本地使用eclipse | IDEA 测试代码。
     *     yarn: hadoop生态圈中的资源调度框架，Spark可以基于Yarn进行调度资源
     *     standalone:Spark自带的资源调度框架，支持分布式搭建，spark可以基于自带的资源调度框架来进行调度。
     *     mesos:资源调度框架。
     *     k8s：虚拟化的方式运行。
     *
     *   2).可以设置在Spark WEBUI中展示的Spark Application的名称
     *   3).可以设置运行的资源情况
     *       主要的资源包含core 和内存
     */
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("wc")
    /**
     * SparkContext 是通往Spark集群的唯一通道
     */
    val sc = new SparkContext(conf)
    val lines :RDD[String] = sc.textFile("./data/words")
    val words :RDD[String] = lines.flatMap(line => {line.split(" ")})
    val pairWords :RDD[(String, Int)] = words.map(word=>{new Tuple2(word, 1)})
    val reduceRDD :RDD[(String, Int)] = pairWords.reduceByKey((v1, v2) => {v1 + v2})
    val result :RDD[(String, Int)] = reduceRDD.sortBy(tp => {tp._2}, false)
    result.foreach(println)
    sc.stop()

    println("----------------------------------------------")

    val conf1 = new SparkConf().setMaster("local").setAppName("spark_scala_wc")
    val sc1 = new SparkContext(conf1)
    sc1.textFile("./data/words").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).foreach(println)
  }
}
