package workflow.model

import workflow.dataset.DataSet

trait ModelPredict {
  
  /**
   * 预测模型接口
   **/
  def rebulitModel(modelPath:String):this.type
  
  def predict(data:DataSet):DataSet

}
