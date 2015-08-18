package workflow.actors

import workflow.WorkActor
import workflow.Workflow
import workflow.inputdata.InputData
import org.apache.spark.SparkContext
import workflow.inputdata.FromHdfs
import workflow.inputdata.FromRDBS
import org.apache.spark.rdd.RDD
import workflow.records.Record
import workflow.records.Property
import workflow.dataset.DataSet
import workflow.mathanalysis.OnlineStatistica
import workflow.dataset.Column
import workflow.dataset.DataType._
import workflow.dataset.ColumnSet
import scala.collection.mutable.HashMap
import workflow.dataset.StoreType._

class ImportDataActor(workflow:Workflow) extends WorkActor(workflow){
  
  
  
  def createImporter:InputData = {
      val linkMethod = getFirstPropertyValue("method").toString
      val sc         = workflow.sc
      linkMethod match {
        
        case "hadoop" =>
          val hdfs      = getFirstPropertyValue("hadoopUrl").toString
          val fileName  = getFirstPropertyValue("fileUrl").toString 
          new FromHdfs(sc, hdfs + "/" + fileName)
        case "rdbs"   =>
          val jdbcDrive = getFirstPropertyValue("importdata.rdbs.driver").toString
          val jdbcUrl   = getFirstPropertyValue("importdata.rdbs.url").toString
          val usrName   = getFirstPropertyValue("importdata.rdbs.username").toString
          val usrPasswd = getFirstPropertyValue("importdata.rdbs.userpasswd").toString
          val sqlConf   = getFirstPropertyValue("importdata.rdbs.sql").toString
          new FromRDBS(sc ,jdbcDrive, jdbcUrl, usrName, usrPasswd, sqlConf)
        case _ =>
          throw new Exception("连接方法输入错误, 没有: " + linkMethod +"方法！")
      }
  }
  
  
  
  /**
   * actor调用接口
   * */
  override def run(){
    if(!hasStarted()){  
      super.run
      val datasplit   = getFirstPropertyValue("dataSplitStr").toString
      println("datasplit:" + datasplit)
      val dataSetStr = try {
           createImporter.load.map(_.split(datasplit))      
      } catch {
        case ex:Exception  =>
          throw new Exception("数据连接参数设置有误，无法取到数据")
      }
      dataSetStr.cache
      val dataCount   = dataSetStr.count
    
      if(dataCount == 0) {
        throw new Exception("数据记录数为零，建议更换数据源在进行操作")
      }    
    
      val dataSet = new DataSet(dataSetStr)
      println("字段数为" + dataSet.dataColumn)
      val record = new Record
      record.addProperty(new Property("data", dataSet))
    
      handleRecord(record)
    }
  }
  

  
}
