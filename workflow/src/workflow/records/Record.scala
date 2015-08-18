package workflow.records


import scala.collection.mutable.ListBuffer


/**
 * <p>
 *    Generic data transport object.  A record consists of an arbitrary list
 *    of name=value string pairs (represented as Property objects)
 * </p>
 * 
 * @author <a href="https://github.com/three4clavin">Sander Gates</a>
 *
 */
class Record {
  
	private var properties: ListBuffer[Property[_]] = new ListBuffer[Property[_]]()
	/**
	 * <p>
	 *    Generic constructor
	 * </p>
	 */
	def this(properties : ListBuffer[Property[_]]){
	    this()
	    this.properties = properties
	}
	
	/**
	 * <p>
	 *    Gets all properties with the specified name
	 * </p>
	 * 
	 * @param name Name of property to find
	 * @return List of properties matching name provided
	 */
	def getProperty(name:String) : ListBuffer[Property[_]] = {
		val ret = new ListBuffer[Property[_]]
		
		if(name == null){
			return ret
		}
		
		for(property <- properties){
			if(name.equals(property.getName())){
				ret += property
			}
		}
		
		ret
	}
	
	/**
	 * <p>
	 *    Returns the first property with a name matching the provided string
	 * </p>
	 * 
	 * @param name Name of property to search for
	 * @return First property found
	 */
	def getFirstProperty(name: String):Property[_] = {
		if(name == null){
			return null;
		}
		
		for(property <- properties){
			if(name.equals(property.getName())){
				return property;
			}
		}
		
		return null;
	}
	
	/**
	 * <p>
	 *    Adds a property to this record
	 * </p>
	 * 
	 * @param property Property to add
	 */
	def addProperty(property: Property[_]){
		properties += property;
	}
	
	/**
	 * <p>
	 *    Removes all properties with the specified name
	 * </p>
	 * 
	 * @param name Name of property to remove
	 */
	def removeProperty(name:String){
		if(name == null){
			return
		}
		
		for(current <- properties){
			if((current.getName() != null) && current.getName().equals(name)){
				properties -= current
			}
		}
	}
	
	/**
	 * <p>
	 *    Removes a specific property
	 * </p>
	 * 
	 * @param property Property to remove
	 */
	def removeProperty(property: Property[_]){
		if((property == null) || (property.getName() == null)){
			return;
		}
		
		properties -= property
	}
	
	/**
	 * <p>
	 *    Returns a list of all properties added to this record
	 * </p>
	 * 
	 * @return All properties added to this record
	 */
	def getAllProperties():List[Property[_]] = {
		properties.toList
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	override def toString(): String = {
		val buffer = new StringBuffer("[Record ");
		for(property <- properties){
			buffer.append(""+property);
		}
		buffer.append("]");
		
		return buffer.toString();
	}
}
