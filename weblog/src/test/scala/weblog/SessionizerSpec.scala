package weblog

import org.scalatest._
import scala.collection.mutable.HashMap

class SessionizerSpec extends FlatSpec with Matchers {

	"ordered List [1, 2, 3, 9, 12, 16, 22]" should "be divided properly" in {
		val list1 = List(1L, 2L, 3L, 9L, 12L, 16L, 22L, Long.MaxValue)
		val sessionHitsMap: HashMap[String, Long] = new HashMap[String, Long]()
		val sessionTimesMap: HashMap[String, Long] = new HashMap[String, Long]()

		val gap = 5L
		val ip = "1.1.1.1"

		Sessionizer.accumSessionHitsAndSessionTimes(Some(ip), list1, sessionHitsMap, sessionTimesMap, gap)

		sessionHitsMap should contain ("1.1.1.1-3" -> 3L)
		sessionHitsMap should contain ("1.1.1.1-16" -> 3L)
		sessionHitsMap should contain ("1.1.1.1-22" -> 1L)

		sessionTimesMap should contain ("1.1.1.1-3" -> 2L)
		sessionTimesMap should contain ("1.1.1.1-16" -> 7L)
		sessionTimesMap should contain ("1.1.1.1-22" -> 0L)


	}

	"ordered List [1, 2]" should "be divided properly" in {
		val list1 = List(1L, 2L, Long.MaxValue)
		val sessionHitsMap: HashMap[String, Long] = new HashMap[String, Long]()
		val sessionTimesMap: HashMap[String, Long] = new HashMap[String, Long]()

		val gap = 5L
		val ip = "1.1.1.1"

		Sessionizer.accumSessionHitsAndSessionTimes(Some(ip), list1, sessionHitsMap, sessionTimesMap, gap)

		sessionHitsMap should contain ("1.1.1.1-2" -> 2L)

		sessionTimesMap should contain ("1.1.1.1-2" -> 1L)

	}

	"ordered List [1]" should "be divided properly" in {
		val list1 = List(1L, Long.MaxValue)
		val sessionHitsMap: HashMap[String, Long] = new HashMap[String, Long]()
		val sessionTimesMap: HashMap[String, Long] = new HashMap[String, Long]()

		val gap = 5L
		val ip = "1.1.1.1"

		Sessionizer.accumSessionHitsAndSessionTimes(Some(ip), list1, sessionHitsMap, sessionTimesMap, gap)

		sessionHitsMap should contain ("1.1.1.1-1" -> 1L)

		sessionTimesMap should contain ("1.1.1.1-1" -> 0L)

	}

	"ordered List without tailing guard" should "be failed return fast" in {
		val list1 = List(1L, 2L, 3L, 9L, 12L, 16L, 22L)
		val sessionHitsMap: HashMap[String, Long] = new HashMap[String, Long]()
		val sessionTimesMap: HashMap[String, Long] = new HashMap[String, Long]()

		val gap = 5L
		val ip = "1.1.1.1"

		Sessionizer.accumSessionHitsAndSessionTimes(Some(ip), list1, sessionHitsMap, sessionTimesMap, gap)

		sessionHitsMap shouldBe empty

		sessionTimesMap shouldBe empty

	}

	"empty List" should "be failed return fast" in {
		val list1 = List()
		val sessionHitsMap: HashMap[String, Long] = new HashMap[String, Long]()
		val sessionTimesMap: HashMap[String, Long] = new HashMap[String, Long]()

		val gap = 5L
		val ip = "1.1.1.1"

		Sessionizer.accumSessionHitsAndSessionTimes(Some(ip), list1, sessionHitsMap, sessionTimesMap, gap)

		sessionHitsMap shouldBe empty

		sessionTimesMap shouldBe empty

		val list2 = Nil
		val sessionHitsMap2: HashMap[String, Long] = new HashMap[String, Long]()
		val sessionTimesMap2: HashMap[String, Long] = new HashMap[String, Long]()

		Sessionizer.accumSessionHitsAndSessionTimes(Some(ip), list2, sessionHitsMap2, sessionTimesMap2, gap)

		sessionHitsMap shouldBe empty

		sessionTimesMap shouldBe empty

	}

	"ordered List [Long.MaxValue, Long.MaxValue]" should "be divided properly also" in {
		val list1 = List(Long.MaxValue, Long.MaxValue)
		val sessionHitsMap: HashMap[String, Long] = new HashMap[String, Long]()
		val sessionTimesMap: HashMap[String, Long] = new HashMap[String, Long]()

		val gap = 5L
		val ip = "1.1.1.1"

		Sessionizer.accumSessionHitsAndSessionTimes(Some(ip), list1, sessionHitsMap, sessionTimesMap, gap)

		sessionHitsMap should contain ("1.1.1.1-"+Long.MaxValue -> 1L)

		sessionTimesMap should contain ("1.1.1.1-"+Long.MaxValue -> 0L)

	}

}