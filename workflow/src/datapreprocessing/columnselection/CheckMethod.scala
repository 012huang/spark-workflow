package datapreprocessing.columnselection

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vectors,Vector}
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.regression.LabeledPoint
import datapreprocessing.util

class CheckMethodByModel(
      targetCol:Int,
      inputCol:Array[Int],
      dataSplitPoint:Double
      ) extends CheckSelectionMethod {
  
  require(!inputCol.isEmpty,"输入字段集合不能为空")
  
  def runCheck(data:RDD[Vector]):Double = {
    val socreAndLabel = createScoreAndLabel(data)
    evaluateMethod(socreAndLabel)
  }
  
  
  //To Be Abstract
  def createScoreAndLabel(data:RDD[Vector]):RDD[(Double,Double)]= {
    val labelData = util.vectorToLabel(data, targetCol, inputCol)
    val (trainingData, testData) = util.simpleLabelData(labelData,dataSplitPoint)
    val modelAl = new LinearRegressionWithSGD
    val model = modelAl.run(trainingData)
    val predict = model.predict(testData.map(_.features))
    predict.zip(testData.map(_.label))
  }
  
  
  
  //To be Abstract
  def evaluateMethod(score:RDD[(Double,Double)]):Double = {
    varianceMethod(score)
  }
  
  def varianceMethod(score:RDD[(Double,Double)]):Double = {
    val (loss,count) = score.map{case(p,l) =>{
      val error = p - l
      (error * error,1L)
    }}.reduce(
        (v1,v2) =>
          (v1._1 + v2._1,v1._2 + v2._2)
       )    
    math.sqrt(loss / count)     
  }
  
}
