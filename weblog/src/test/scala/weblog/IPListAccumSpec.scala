package weblog

import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer
import org.scalatest._

class IPListAccumSpec extends FlatSpec with Matchers {

  it should "return an empty mutable HashMap with its zero method" in {
    val initialHashMap = IPListAccum.zero(null)

    initialHashMap shouldBe a [HashMap[_, _]]
  }

  "The result of addAccumulator" should "contain the new added (String, Long) pair in the HashMap" in {
    val preparedHashMap = IPListAccum.zero(null)
    val ip1: String = "1.1.1.1"
    val listB1: ListBuffer[Long] = ListBuffer()
    listB1 += 10L
    listB1 += 20L
    listB1 += 30L
    listB1 += 50L
    listB1 += 80L
    listB1 += 90L

    preparedHashMap.put(ip1, listB1)

    val ip2: String = "2.2.2.2"
    val listB2: ListBuffer[Long] = ListBuffer()
    listB2 += 100L
    listB2 += 200L
    listB2 += 300L
    listB2 += 500L
    listB2 += 800L
    listB2 += 900L
    preparedHashMap.put(ip2, listB2)

    val tuple1: Tuple2[String, Long] = ("1.1.1.1", 101L)
    val tuple2: Tuple2[String, Long] = ("1.1.1.1", 102L)

    val tuple3: Tuple2[String, Long] = ("3.3.3.3", 102L)

    val result1 = IPListAccum.addAccumulator(preparedHashMap, tuple1)
    result1 should contain ("1.1.1.1" -> (listB1 += 101L))

    val result2 = IPListAccum.addAccumulator(preparedHashMap, tuple2)
    result2 should contain ("1.1.1.1" -> (listB1 ++= Array(101L, 102L)))

    val result3 = IPListAccum.addAccumulator(preparedHashMap, tuple3)
    result3 should contain ("3.3.3.3" -> ListBuffer(102L))

  }

  "The result of addInPlace" should "combine the contents of both hashmaps" in {
    val preparedHashMap1 = IPListAccum.zero(null)
    val ip1: String = "1.1.1.1"
    val listB1: ListBuffer[Long] = ListBuffer()
    listB1 += 10L
    listB1 += 20L
    listB1 += 30L
    listB1 += 50L
    listB1 += 80L
    listB1 += 90L

    preparedHashMap1.put(ip1, listB1)

    val preparedHashMap2 = IPListAccum.zero(null)
    val ip2: String = "2.2.2.2"
    val listB2: ListBuffer[Long] = ListBuffer()
    listB2 += 100L
    listB2 += 200L
    listB2 += 300L
    listB2 += 500L
    listB2 += 800L
    listB2 += 900L
    preparedHashMap2.put(ip2, listB2)

    val result1 = IPListAccum.addInPlace(preparedHashMap1, preparedHashMap2)
    result1 should contain (ip1 -> listB1)
    result1 should contain (ip2 -> listB2)

    val result2 = IPListAccum.addInPlace(preparedHashMap2, preparedHashMap1)
    result2 should contain (ip1 -> listB1)
    result2 should contain (ip2 -> listB2)
  }
}