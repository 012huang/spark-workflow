package workflow.dataset

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import workflow.mathanalysis.OnlineStatistica
import org.apache.spark.mllib.linalg.{Vectors,Vector}
import scala.collection.mutable.HashMap
import workflow.mathanalysis.DataManage
import scala.collection.mutable.HashSet
import scala.collection.mutable.ArrayBuffer


class LabeledData(
    var data:RDD[LabeledPoint],
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
	
	def setData(newData:RDD[LabeledPoint]) = data = newData
	def getData = data
	
	def setColumnSet(newCol:ColumnSet) = columnTop = newCol
	def getColumnSet = columnTop
	
	/**
	 * 设置字段为输入角色
	 * */
	def setInputRole  {
	  if(!inputName.isEmpty) {
	    columnTop.setInputRole(inputName)
	    println("配置输入列: "+inputName.mkString(",")+" 成功")
	  }
	  else println("输入字段集合为空值，设置输入角色失败！")
	}
	
	/**
	 * 设置字段为目标角色
	 * */
	def setTargetRole {
	  if(targetName != null) {
	    columnTop.setTargetRole(targetName)
	    println("配置目标列: "+targetName+" 成功")
	  }
	  else println("目标字段集合为空值，设置目标角色失败！")
	}
	
	/**
	 * 设置字段属性为分类类型
	 * */
	def setCategoricalColumn(columnName:Array[String]) = {
	  if(!columnName.isEmpty){
	    columnTop.setCategoricalColumn(columnName)
	  } else println("设置分类字段的集合为空！")	  
	}
	
	/**
	 * 设置字段属性为连续类型
	 * */
	def setContinuousColumn(columnName:Array[String]) = {
	  if(!columnName.isEmpty){
	    columnTop.setContinuousColumn(columnName)
	  } else println("设置连续字段的集合为空！")	  
	  
	}
	
	/**
	 * 设置字段属性为ID类型
	 * */
	def setIDColumn(columnName:Array[String]) = {
	  if(!columnName.isEmpty){
	    columnTop.setIDColumn(columnName)
	  } else println("设置ID字段的集合为空！")	  
	  
	}
	
	def setAllColumnToContinuous = columnTop.setAllColumnToContinuous
	
	
	def setAllColumnToInput = columnTop.setAllColumnToInput
	
    def setCategoricalFeaturesInfo(idWithMap:Array[(Int,HashMap[String,Int])]){
	  columnTop.setCategoricalFeaturesInfo(idWithMap)
	}	
	
/**
 * 设置选择的特征值
 * */	
	def setChooseFeature(FeatureId:Array[Int]) = {
	  val dataCom = new DataManage(columnTop)
	  data = dataCom.LabeledChangeFeature(data, FeatureId)
	  val newColumn = columnTop.filter(col =>FeatureId.contains(col.id) || col.isTarget)
	  val len = newColumn.size
      var j = 0
      val newColumns = for(column<- newColumn.getColumn if !column.isVoid) yield{
        var newColumn:Column = null
        if(column.isInput) {
          column.id = j
          newColumn = column
          j +=1
        } 
        if(column.isTarget) {
          column.id = len - 1
          newColumn = column
        }
        newColumn
      }

	  columnTop.changeAllColumn(newColumns)
	}
/**
 * 过滤掉所选的特征值
 **/
	def filterFeature(FeatureId:Array[Int]) = {
	  val dataCom = new DataManage(columnTop)
	  data = dataCom.LabeledFilterFeature(data, FeatureId)
	  val newColumn = columnTop.filter(col => !FeatureId.contains(col.id) || col.isTarget)
	  val len = newColumn.size
      var j = 0
      val newColumns = for(column<- newColumn.getColumn if !column.isVoid) yield{
        var newColumn:Column = null
        if(column.isInput) {
          column.id = j
          newColumn = column
          j +=1
        } 
        if(column.isTarget) {
          column.id = len - 1
          newColumn = column
        }
        newColumn
      }
	  columnTop.changeAllColumn(newColumns)
	}
	
/**
 * 连续字段离散化 ， 依据正态分布分为5类
 **/
	
   def AnalysisContinuousColumn(targetClass:Int = 1) {
     println("连续字段特征分析:")
     val col = columnTop.getIdOfByCondition(
         f =>{
       f.isContinuous && f.isDoubleType && f.isInput
     })
     
     val Max = columnTop.getMaxWithColumnId(col)
     val Min = columnTop.getMinWithColumnId(col)
     val maxAndMin = new HashMap[Int,(Double,Double)]
     (Max.zip(Min)).foreach{
       case(max,min) => maxAndMin(max._1) = (max._2,min._2)
     }
     
     val analysisData = data
    
     println("分析数据量为:" + analysisData.count, 2)
     val dataCom = new DataManage(columnTop)
     val (variance,mean) = dataCom.getVariance(analysisData, col ,maxAndMin)
     
     val names = ArrayBuffer[String]()
     val tempMin = ArrayBuffer[String]()
     val tempMax = ArrayBuffer[String]()
     val title = Array("字段名","平均值","方差","最大值","最小值")
     for(i <- 0 until col.length) {
       names += columnTop.idToName(col(i))
       val (max,min) = maxAndMin(col(i))
       tempMin += "%.4f".format(min)
       tempMax += "%.4f".format(max)
     }

     val value = Array(mean.map("%.4f".format(_)).toArray,variance.map("%.4f".format(_)).toArray,tempMax.toArray,tempMin.toArray)
//     logger.titlePrint("连续字段的平均值，方差，最大值，最小值:", 2)
      
     val zdata = dataCom.z_Trainsformation(analysisData, col, mean, variance)
     val valueStatis = dataCom.FindOutlierValueInContinous(zdata, 1, col)
     val columnName = columnTop.getColumnName
     AnalysisStatis(valueStatis)
     
     
     
//     logger.titlePrint("连续字段流失分析结束",2)     
   }
	
	
	
	
/**
 * 过滤掉异常分类字段
 * */	
	def FilterOutlierValueInCategorical(targetClass:Int = 1, ratio:Double = 0.8) = {
      val columnName = columnTop.getColumnName
//	  val threshold = 0.8
	  val dataCom = new DataManage(columnTop)
	  val valueStatis = dataCom.FindOutlierValueInCategorical(data, targetClass)
	  
//	  valueStatis.foreach{
//        case(index,cmap) => 
//          println("ID" + index.toString)
//          val tt = cmap.map(f=>Array(f._1,f._2._1,f._2._2).map(_.toString)).toArray
//          logger.chartPrint(tt)
//      }
	  
//	  logger.titlePrint("分类字段特征分析")
	  AnalysisStatis(valueStatis)
//	  println("分类异常字段处理:")
	  val fliterValue = valueStatis.map{
	    case(index,map) => {
	      val set = new HashSet[Double]
	      val categoricalMap = columnTop.getCategoricalInfoById(index).map{case(key,value) => (value,key)}
	      val filterMap = map.foreach{
	        case(key,(rightCnt,totalCnt)) => {
	          val weight = rightCnt.toDouble / totalCnt
	          if(weight > ratio) {
	            val value = categoricalMap(key.toInt)
	            val name = columnName(index)
//	            println("过滤: %s字段中%s类别,对应目标列类别%d,占比%.4f > 阈值%.4f".format(name,value,targetClass,weight,ratio))
	            set += key
	          } 
	        }
	      }
	      (index,set)
	     }
	   }
//	  logger.titlePrint("分类字段流失分析结束",2)
//	  data = dataCom.FilterValueInCategorical(data, fliterValue)
	  
	}
	

	def getFeatureStatis:OnlineStatistica = {
	    data.aggregate(new OnlineStatistica)(
	        (agg, value) => {agg.add(value.features)},
	        (agg1, agg2) => agg1.merge(agg2))
	}
	
	
	private def AnalysisStatis(valueStatis:Array[(Int,HashMap[Double,(Int,Int)])], targetClass:Int = 1) = {
	  val columnName = columnTop.getColumnName
//	  //test
//	  val infoColumn = columnTop.getColumn
//	  val info = infoColumn.map(f=>Array(f.columnName,f.id.toString,f.dataType.toString))
//	  val valueid = valueStatis.map(_._1.toString)
//	  val catcol = columnTop.getAllIdOfCategorical
//	  logger.chartPrint(info)
//	  println(catcol.mkString(","))
//	  val catmap = columnTop.getCategoricalInfoById(catcol)
//	  catmap.foreach{case(id,cmap)=>{
//	    println("ID " + id.toString + "字段名:" + columnName(id))
//	    if(!cmap.isEmpty){
//	       val litmap = cmap.map(f=>Array(f._1,f._2.toString)).toArray
//	       logger.chartPrint(litmap)
//	    }  else println("MAP 为空")
//	  }}
//	  logger.chartPrint(Array(valueid,catcol))
//	  println(valueid.mkString(","))
	  //test
	  val fliterValue = valueStatis.foreach{
	    case(index,map) => {
	      val set = new HashSet[Double]
	      //@test
	      val sum1 = map.values.map(_._1).sum
	      val sum0 = map.values.map(_._2).sum - sum1
	      val testname = columnName(index)
	      println("ID: "+index+" 字段名: "+ testname + " 类别 1 数目:" + sum1 + "  类别 0 数目:" + sum0)
	      val test = map.toArray
	      val temp = test.sortBy{
	        case(key,(rightCnt,totalCnt)) =>
	          rightCnt.toDouble / sum1
	        }
	      
//	      logger.chartPrint(temp.map(f=>Array(f._1.toString,f._2._1.toString,f._2._1.toString)))

	      val limit = math.min(temp.length , 5)
	      val temlen = temp.length - 1
	      val titleNme = Array("类别","类别1数目","类别0数目","占流失类别比例","占当前流失客户比例")
	      val list = ArrayBuffer[Array[String]]()
	      list += titleNme
	      val repMap = columnTop.getCategoricalInfoById(index).map{case(key,value) => (value,key)}
	//      logger.chartPrint(repMap.toArray.map(f => Array(f._1.toString,f._2)))
	      val seq = for(i<- 0 until limit) yield{
	        val num = temlen - i
            val pre1 = temp(num)._2._1.toDouble / sum1
            val pre2 = temp(num)._2._1.toDouble / temp(num)._2._2
	        val classValue = if(columnTop.judgeType(index, _.isCategorical)) repMap(temp(num)._1.toInt) else temp(num)._1.toString
	        list += Array(classValue, temp(num)._2._1, temp(num)._2._2 - temp(num)._2._1, "%.4f".format(pre1) ,"%.4f".format(pre2)).map(_.toString)
//	        printf("%s字段流失率排名第  %d 中的 %s 类别,对应目标列类别 %d ,占总流失客户比离为 %.4f ,占当前类别流失客户比例为 %.4f \n"	,testname, i+1 , classValue, targetClass ,pre1,pre2)
	      }	      
//	      logger.chartPrint(list.toArray)
	    }
     }
	  
	}
}
