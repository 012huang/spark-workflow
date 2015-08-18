package workflow.dataset

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vectors,Vector}
import workflow.mathanalysis.OnlineStatistica
import workflow.mathanalysis.util

class VectorData(
    var data : RDD[Vector],
    var columnTop:ColumnSet
    ) extends DataInfo{
  
    var inputName: Array[String] = _
    var targetName:String = _

  
    def cache{
	  data.cache
	}
	
	def unpersist(blocking:Boolean){
	  data.unpersist(blocking)
	}
	
	def setData(newData:RDD[Vector]) = data = newData
	def getData = data
	
	def setColumnSet(newCol:ColumnSet) = columnTop = newCol
	def getColumnSet = columnTop
	
	def getStatis:OnlineStatistica = {
	    data.aggregate(new OnlineStatistica)(
	        (agg, value) => agg.add(value),
	        (agg1, agg2) => agg1.merge(agg2))
	}
	
    def toLabeledData:LabeledData = {
	  val inputCol = columnTop.getAllIdOfInput
	  val targetCol = columnTop.getIdOfTarget
	  if(!inputCol.isEmpty){
	    val rddLabel = data.map(value =>
	        { 
	          val label = util.vectorToLabel(value, targetCol, inputCol)
	          label
	        } 
	      )
	      new LabeledData(rddLabel,columnTop)	  
	  } else {
	    throw new Exception("没有目标列 ， 或没有输入列无法转换成标签数据！")
	  }
	  
    }
    
    
    def filterValue(fun1:Double => Boolean,
	                fun2:ColumnSet => Int){
	  val flag = fun2(columnTop)
	  data = data.filter(value => fun1(value(flag)))
	}
	
	/**
	 * 过滤字段
	 * */
	def filterColumn(stayColumn:Array[Int]):this.type = {
	  data = data.map(
	      value => {
	        val tem = for(elem<- stayColumn) yield value(elem)
	        Vectors.dense(tem)
	        }
	  )
	  columnTop = columnTop.filter(
	      col => 
	        stayColumn.contains(col.id)
	  ).refreshColumnId	  
	  this
	}
	
	/**
	 * 按条件过滤字段
	 * */
	def filterColumnWithCondition(f:Column => Boolean):this.type = {
	  this.columnTop = this.columnTop.filter(f)
	  val stayColumn = this.columnTop.getAllId
	  data = data.map(
	      value =>{
	        val tmp = for(elem <- stayColumn) yield value(elem)
	        Vectors.dense(tmp)
	      }
	  )	  
	  this
	}
	

  

}
