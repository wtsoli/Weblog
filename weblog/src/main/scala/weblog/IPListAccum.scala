//C:\devmyself\spark_dev\IPListAccum.scala
package weblog
import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer

import org.apache.spark.AccumulableParam

object IPListAccum extends AccumulableParam[HashMap[String, ListBuffer[Long]], (String, Long)] {

	def addAccumulator(r: HashMap[String, ListBuffer[Long]], t: (String, Long)): HashMap[String, ListBuffer[Long]] = {
		mergeIntoMap(r, t);
	}

	def addInPlace(r1: HashMap[String, ListBuffer[Long]], r2: HashMap[String, ListBuffer[Long]]): HashMap[String, ListBuffer[Long]] = {
		mergeMap(r1, r2);
	}

	def zero(initialValue: HashMap[String, ListBuffer[Long]]): HashMap[String, ListBuffer[Long]] = {
    	new scala.collection.mutable.HashMap[String, ListBuffer[Long]]()
  	}

	private[this] def mergeMap(map1: HashMap[String, ListBuffer[Long]], map2: HashMap[String, ListBuffer[Long]]): HashMap[String, ListBuffer[Long]]  = {
		val mergedMap = map1 ++ map2.map {
  		case (ip, lst2) => ip -> {
        val lst1 = map1.getOrElse(ip, ListBuffer()).asInstanceOf[ListBuffer[Long]]
        lst1 ++= lst2
      }
  	}
  	mergedMap.asInstanceOf[HashMap[String, ListBuffer[Long]]]
	}

	private[this] def mergeIntoMap(r: HashMap[String, ListBuffer[Long]], t: (String, Long)): HashMap[String, ListBuffer[Long]] = {
    val lst = r.getOrElse(t._1, ListBuffer()).asInstanceOf[ListBuffer[Long]]
		r.put(t._1 , lst += t._2)
		r
	}

}