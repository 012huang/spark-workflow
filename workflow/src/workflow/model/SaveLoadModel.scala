package workflow.model

import scala.collection.mutable.HashMap
import java.io.PrintWriter
import java.io.IOException
import java.io.OutputStreamWriter
import java.io.FileOutputStream
import scala.io.Source

trait SaveLoadModel {
  
  val params :HashMap[String,String]
  
  def saveParams(savePath:String) {
    println("模型保存路径为" + savePath)
    if(!params.isEmpty && savePath!= null){
      try{
        val out = new PrintWriter(new OutputStreamWriter(new FileOutputStream(savePath),"UTF-8"))
        for(elem <- params) out.println(elem._1 +"=" + elem._2)
        out.close()
      }catch{
        case ex:IOException => throw new Exception("the save params path is not right!")
      }      
    }
  }
  
  def readParams(savePath:String) {
    if(savePath != null){
      params.clear
      try{
        val readIn = Source.fromFile(savePath,"UTF-8").getLines
        for(elem <- readIn) {
          val (key,value) = {
            val temp = elem.split("=")
            (temp(0),temp(1))
          }
          params(key) = value
        }
      }catch{
        case ex:IOException => throw new Exception("the save params path is not right!")
        
      }
    }
  }
  

}
