package workflow.actors

import workflow.WorkActor
import workflow.Workflow
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

class StartActor(workflow: Workflow) extends WorkActor(workflow){
  
  override def run(){
    if(!hasStarted){
      super.run
      val cf = new SparkConf()
      val sparkMaster  = this.getFirstPropertyValue("masterStr").toString
      val sparkAppName = this.getFirstPropertyValue("appName").toString
      val sparkCors    = this.getStringPropertyValueOrElse("spark.cores.max" , "5")           
      val sparkMemory  = this.getStringPropertyValueOrElse("spark.executor.memory" , "5g")
      cf.setMaster(sparkMaster)
      cf.setAppName(sparkAppName)
      cf.set("spark.cores.max", sparkCors)
      cf.set("spark.executor.memory", sparkMemory)
      val sc = new SparkContext(cf)
      workflow.sc = sc
    }
  }

}
