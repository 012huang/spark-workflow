package workflow.model

import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer
import workflow.records.Property

class ModelFactory(modelStr:String){
  
  def createModel(modelParams:HashMap[String,String]):DataLearning = {
    modelStr match{
      case "kmeans" => new TrainKMeans(modelParams)
      case "logisticregression" => new TrainLogisticRegression(modelParams)
      case "naivebayes" => new TrainNaiveBayes(modelParams)
      case "svm" => new TrainSVM(modelParams)
      case "linearregression" => new TrainLinearRegression(modelParams)
      case "redgeregression" => new TrainRidgeRegression(modelParams)
      case "lasso" => new TrainLasso(modelParams)
      case "decisiontree" => new TrainDecisionTree(modelParams)
      case "gradientboostedtrees" => new TrainGradientBoostedTrees(modelParams)
      case "randomforest" => new TrainRandomForest(modelParams)
      case _ => throw new Exception("模型选择有误，没有: " + modelStr + " 模型")
    }
  }
  
  def getModel(properties:ListBuffer[Property[_]]):DataLearning = {
    val params = propertyToHashMap(properties)
    createModel(params)
  }
  
  private def propertyToHashMap(properties:ListBuffer[Property[_]]):HashMap[String,String] =  {    
    
    val params = new HashMap[String,String]()    
    for(propertie <- properties if propertie.getName.startsWith("model")) {
       params(propertie.getName) = propertie.getValue.asInstanceOf[String]            
    }
    params    
  }

}
