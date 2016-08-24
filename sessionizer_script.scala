import scala.collection.mutable.HashMap
import scala.collection.mutable.Map
import scala.collection.mutable.ListBuffer

import org.apache.spark.AccumulableParam

import java.text.SimpleDateFormat

/*
 * accumulator for collecting the dataset into a HashMap ( ip -> <list of timestamp in Long>)
 * here to fit the dataset into memory in single machine -- the only one I got myself :)
 */
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

//"C:\\devmyself\\spark_dev\\2015_07_22_mktplace_shop_web_log_sample.log"
//implicit val tuple: Tuple2[String, String] = ("C:\\devmyself\\spark_dev\\2015_07_22_mktplace_shop_web_log_sample.log", "C:\\devmyself\\spark_dev\\result.txt")
// :load C:\devmyself\spark_dev\sessionizer_script.scala
val logFileFullPath = implicitly[Tuple2[String, String]]._1
val outputFileFullPath = implicitly[Tuple2[String, String]]._2

import java.nio.file.{Paths, Files}
if ( !Files.exists(Paths.get(logFileFullPath)) ) {
	println("The file " + logFileFullPath + " does not exist." )
	System.exit(1)
}

val textFile = sc.textFile(logFileFullPath)
val ipTimeTuples = textFile.map { row =>
	val eles = row.split(" ")
	val ip = eles(2)
	val timestamp = eles(0)
	(ip.substring(0, ip.indexOf(":")), timestamp)
	//(ip.substring(0, ip.indexOf(":")), 1)
}




val testListAccum = sc.accumulable(new scala.collection.mutable.HashMap[String, ListBuffer[Long]](), "test list accum")(IPListAccum)
ipTimeTuples.foreach { x =>
	val datestr = x._2.substring(0, x._2.indexOf('.')).replace('T', ' ') //2015-07-22T05:13:04.816465Z
	// datestr now is "2015-07-22 05:13:04"
	val dateIt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(datestr)
	testListAccum += (x._1, dateIt.getTime())
}



// collecting the result for each ip session-wise,
// key: <11.22.33.44-<timestamp in Long>  value: <how many hits in this session in Long>
val resultMap = new scala.collection.mutable.HashMap[String, Long]()

// collecting the result for each ip session-wise,
// key: <11.22.33.44-<timestamp in Long>  value: <how long the ip/session is in Long(milli secs)>
val sessionTimeMap = new scala.collection.mutable.HashMap[String, Long]()

// global value for the session interval, right now it is 15 mins
val sessionInterval = 900000L

// make sure the timestamp list for each Map entry is ordered
// to facilitate the linear processing for separate the sessions for each ip
val sortedListAccum = testListAccum.value.map { entry =>
	(entry._1, entry._2.sorted)
}

// Here this function is one only for its side-effects(filling two hashmaps)
// since in the standalone app, this will be a private method used internally,
// so should be fine with its impurity.
def accumSessionHitsAndSessionTimes(ipAddr: Option[String],
			timestampLst: List[Long], 
			sessionHitsMap: HashMap[String, Long],
			sessionTimesMap: HashMap[String, Long]): Unit = {
	ipAddr match {
		case None => return
		case Some(ip) => {
			if (timestampLst.isEmpty) {
					return
			}
			if (timestampLst.last != Long.MaxValue) { // require the last element is the max Long
				return
			}
			if (timestampLst.size == 1 ) {
				println("WTSOLID: actually here for ip: " + ip)
				sessionHitsMap.put(ip, 0)
			} else if (timestampLst.size == 2) {
				//println("WTSOLID: only one hit globally for ip: " + ip)
				sessionHitsMap.put(ip + "-" + timestampLst.head, 1L)
				sessionTimesMap.put(ip + "-" + timestampLst.head, 0L)
			} else {
				var counter = 0L
				var sentinel = timestampLst.head
				for (List(left,right) <- timestampLst.sliding(2)) {
					counter += 1
					if (right - left > sessionInterval) {
						sessionHitsMap.put(ip + "-" + left, counter)
						counter = 0L

						sessionTimesMap.put(ip + "-" + left, left - sentinel)
						sentinel = right
					}
				}
			}
		}
	}
}

// materialize the resultMap
// key: <ip>-<timestamp for first hit for this session>
// value: number of hits for this ip/session
sortedListAccum.foreach { entry =>
	val ip = entry._1
	//val timestamps = entry._2
	val timestampLst = (entry._2 += Long.MaxValue).toList
	accumSessionHitsAndSessionTimes(Some(ip), timestampLst, resultMap, sessionTimeMap)
}

// verify the results
val verifyMap = new scala.collection.mutable.HashMap[String, Long]()
resultMap.foreach { entry =>
	val ipWithTimeStamp = entry._1
	val ip = ipWithTimeStamp.substring(0, ipWithTimeStamp.indexOf("-"))

	val value = verifyMap.getOrElse(ip, 0L)
	verifyMap.put(ip, value + 1L)

}

println(verifyMap.valuesIterator.max) //13
println(verifyMap.valuesIterator.min) //1

// 1. how many sessions analysied
println("1 The number of ip/session(s) in the weblog: ")
val numberOfSessions = sessionTimeMap.size
println("The number of ip/session(s) in the weblog: " + numberOfSessions)
println("")
println("")


// 2. Determine the average session time
println("2 The average session time(in minutes): ")
val averageSessiontime = sessionTimeMap.toSeq.map(_._2).sum / sessionTimeMap.size
println("The average session time(in minutes): " + averageSessiontime / (60 * 1000.0))
println("")
println("")


// 3. Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.
println("3 Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.")
println(sessionTimeMap.toSeq.map(_._2).filter(_ == 0L).size +" ip/session(s) have only one hit to one URL")
println("")
println("")

// 4. Find the most engaged users, ie the IPs with the longest session times
// The longest session time for ip:
println("4 The 100 longest ip/session and the length in milli seconds: ")
val longest100IPSession = sessionTimeMap.toSeq.sortBy(_._2).reverse.take(100)
println("The 100 longest ip/session and the length in milli seconds: \n" + longest100IPSession )
println("")
println("")