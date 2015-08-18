package datapreprocessing.datatrainsformation

import org.apache.spark.mllib.linalg.{Vector,Vectors}

class FractionalTrainsformer(
      max:Array[Double],
      min:Array[Double]
      ) extends VectorTransformer {
  
  
  def transformer(value:Vector):Vector = {
    val proPoint = getProporation
    val newValue = for(i<- 0 until value.size) yield value(i) * proPoint(i)
    Vectors.dense(newValue.toArray)
  }
  
  
  /**
   * 得到小数定标点
   * @return 小数定标点
   * */
  def getProporation:Array[Double] = {
    val maxAbs = max.toArray.map(_.abs)
    val minAbs = min.toArray.map(_.abs)
    val proporation = {for(i<- 0 until maxAbs.length) yield {
      if (maxAbs(i)>minAbs(i)) {
        var pro:Double= 1
        val temp = maxAbs(i)
        var tempdata= temp
        while(tempdata<0.1 || tempdata >1){
             while(tempdata<0.1) {
               tempdata = temp
               pro = pro*10
               tempdata = tempdata * pro
               }
             while(tempdata>1){
               tempdata = temp
               pro = pro/10
               tempdata = tempdata *pro
             }
          }
        pro
        }
      else {
        var pro = 1.0
        val temp = minAbs(i)
        var tempdata = temp
        while(tempdata<0.1 || tempdata >1){
             while(tempdata<0.1) {
               tempdata = temp
               pro = pro*10
               tempdata = tempdata * pro
               }
             while(tempdata>1){
               tempdata = temp
               pro = pro/10
               tempdata = tempdata *pro
             }
          }
        pro
        }
    }}.toArray
    proporation
  }

  
}
