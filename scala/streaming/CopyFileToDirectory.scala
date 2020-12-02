package streaming

import java.io.{BufferedReader, BufferedWriter, File, FileReader, FileWriter}
import java.util.UUID

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/12/1 22:54
*   @Description : 
*
*/
/**
 * 将项目中的 ./data/copyFileWord 文件 每隔5s 复制到 ./data/streamingCopyFile 路径下
 */
object CopyFileToDirectory {

  // construct the IO interface
  var fr: FileReader = _
  var fw: FileWriter = _
  var br: BufferedReader = _
  var bw: BufferedWriter = _

  def main(args: Array[String]): Unit = {
    while(true){
      Thread.sleep(5000)
      val uuid = UUID.randomUUID().toString()
      println(uuid)

    }
  }

  /**
   * Copy the file to corresponding directory
   */
  def copyFile(fromFile: File, toFile: File): Unit ={
    try {
      fr = new FileReader(fromFile)
      fw = new FileWriter(toFile)
      // use buffer
      br = new BufferedReader(fr)
      bw = new BufferedWriter(fw)
      var tempString = br.readLine()
      while(tempString != null){
        bw.write(tempString)
        bw.newLine()
        tempString = br.readLine()
      }
    } catch {
      case e:Exception=>{
        println("error == "+e.printStackTrace())
      }
    } finally {
      if(bw != null) bw.close()
      if(br!= null) br.close()
      if(fw != null) fw.close()
      if(fr != null) fr.close()
    }
  }
}
