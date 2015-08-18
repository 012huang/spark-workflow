package workflow.dataset

import java.util.logging.Logger

trait DataInfo extends Serializable{
  
  var logger:Logger =_
    
  var id:Int = _
  
  var dataName:String = _
  
  var dataRow:Long = 0
  
  var dataColumn:Int = 0
  
  var splitStr: String = _
  
  var includeColumnName = false

}
