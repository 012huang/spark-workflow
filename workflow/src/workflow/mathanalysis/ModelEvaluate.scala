package workflow.mathanalysis

import org.apache.spark.rdd.RDD

object ModelEvaluate {
  
  /**return (Long,Long,Long),the first number is the total number of the predicting right,
   * the second number is the total number of the predicting wrong. the last number is the 
   * data count number
   * */
  def countRigthAndWrong(predictionAndLabel:RDD[(Double,Double)]):(Long,Long,Long) ={
    val count = predictionAndLabel.map{case(score,label) =>{
      if(score == label) (1L,0L,1L)
      else (0L,1L,1L)
    }}.reduce((a,b)=>{
      (a._1+b._1,a._2+b._2,a._3+b._3)
    })
    count
  }
  
  /**
   * predicting right precent and wrong precent
   **/
  def preAccuracy(count:(Long,Long,Long)):(Double,Double) = {
    (count._1.toDouble / count._3, count._2.toDouble /count._3)
  } 

}
