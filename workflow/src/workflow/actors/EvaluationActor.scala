package workflow.actors

import workflow.Workflow
import workflow.WorkActor
import workflow.evaluation.Evaluation
import workflow.records.Record
import workflow.evaluation.ROCEvaluation
import workflow.evaluation.LossEvaluation
import workflow.dataset.DataSet

class EvaluationActor(workflow :Workflow) extends WorkActor(workflow){
  
  private var evaluater  :Evaluation   =_
  private var dataSet    :DataSet      =_

//  private var 
  
  
  def createEvaluation :Evaluation = {
    evaluater = getFirstPropertyValue("method").toString match {
      case "roc"      => new ROCEvaluation 
      case "variance" => new LossEvaluation
      case _          => 
        throw new Exception("评估方法选择错误！")
        
    }
    evaluater
  }
  
  override def run(){
    
    if(!hasStarted) {
      super.run()
      val result = createEvaluation.evaluation(dataSet)
      println("工作流结束")
      //TO DO
    }
  }
  
  override def handleRecord(record: Record){
    this.dataSet = try{
      record.getFirstProperty("data").getValue.asInstanceOf[DataSet].clone
    } catch {
      case ex:Exception =>
        throw new Exception("上游节点数据获取错误！")
    }     
  }

}
