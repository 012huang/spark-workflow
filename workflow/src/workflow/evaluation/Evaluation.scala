package workflow.evaluation

import workflow.dataset.DataSet
import scala.collection.mutable.HashMap
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.rdd.RDD

trait Evaluation {
  //评估结果以HashMap的方式取回
  def evaluation(dataset:DataSet):HashMap[String, String]  
  
 
  def getValidationData[U](dataset:DataSet , resultSet: U => Unit){
    val preAndLabel = dataset.getData.map(
        value =>{
          val len = value.length
          (value(len - 1).toDouble , value(len - 2).toDouble)
        }
      )
    resultSet(preAndLabel.asInstanceOf[U])
  }

}

/**
 * 
 * */
class ROCEvaluation extends Evaluation{  
  
  def evaluationROC(preAndLabel:RDD[(Double,Double)]):HashMap[String, String] = {
     
    val result:HashMap[String ,String] = new HashMap[String ,String]()
    val eval = new MulticlassMetrics(preAndLabel)
    val confusionMatrix = eval.confusionMatrix
    result("confusionmatrix") = confusionMatrix.toArray.mkString(",")
    
    val labels = eval.labels
    for(elem<- labels){
	    val falsePositive = eval.falsePositiveRate(elem)
	    val truePositive = eval.truePositiveRate(elem)
	    val fMeasure = eval.fMeasure(elem)
	    val precision = eval.precision(elem)
	    val recall = eval.recall(elem)
	    
	    result("falsePositive" + elem) = falsePositive.toString
	    result("truePositive" + elem) = truePositive.toString
	    result("fMeasure" + elem) = fMeasure.toString
	    result("precision" + elem) = precision.toString
	    result("recall" + elem) = recall.toString
	    
    }
    result("wfalsepositiverate") = eval.weightedFalsePositiveRate.toString
    result("wtruepositiverate") = eval.weightedTruePositiveRate.toString
    result("wfmeasure") = eval.weightedFMeasure.toString
    result("wprecision") = eval.weightedPrecision.toString
    result("wrecall") = eval.weightedRecall.toString
    result("fmeasure") = eval.fMeasure.toString
    result("precision") = eval.precision.toString
    result("recall") = eval.recall.toString
    result
  }
  
  
  def evaluation(dataset:DataSet):HashMap[String, String] = {
    var testdata:RDD[(Double,Double)] = null
    getValidationData(dataset, (temp:RDD[(Double,Double)]) => testdata = temp)
    evaluationROC(testdata)
  }
  
}



class LossEvaluation extends Evaluation {
  
  def evaluationLoss(dataset: RDD[(Double,Double)]) :HashMap[String ,String] = {
    val (loss,count) = dataset.map{case(p,l) =>{
      val error = p - l
      (error * error,1L)
    }}.reduce(
        (v1,v2) =>
          (v1._1 + v2._1,v1._2 + v2._2)
       ) 
    val result:HashMap[String ,String] = new HashMap[String ,String]()
     
    
    result("loss") = math.sqrt(loss / count).toString  
    result
  }
  //评估接口
  def evaluation(dataset:DataSet):HashMap[String, String] = {
    var testdata:RDD[(Double,Double)] = null
    getValidationData(dataset, (temp:RDD[(Double,Double)]) => testdata = temp)
    evaluationLoss(testdata)
  }
}
