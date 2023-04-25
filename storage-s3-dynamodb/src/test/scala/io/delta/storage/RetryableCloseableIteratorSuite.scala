package io.delta.storage

import java.io.UncheckedIOException
import java.util.function.Supplier

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.s3a.RemoteFileChangedException
import org.scalatest.funsuite.AnyFunSuite

// scalastyle:off println
class RetryableCloseableIteratorSuite extends AnyFunSuite {

  private def getIter(
      range: Range,
      throwAtIndex: Option[Int] = None): CloseableIterator[String] =
    new CloseableIterator[String] {
      var index = 0
      val impl = range.iterator.asJava

      override def close(): Unit = { }

      override def hasNext: Boolean = {
        if (throwAtIndex.contains(index)) {
          println(s"`hasNext` throwing for index $index")
          throw new RemoteFileChangedException(s"path -> index $index", "operation", "msg");
        }

        impl.hasNext
      }

      override def next(): String = {
        if (throwAtIndex.contains(index)) {
          println(s"`next` throwing for index $index")
          throw new RemoteFileChangedException(s"path -> index $index", "operation", "msg");
        }

        index = index + 1

        impl.next().toString
      }
    }

  /**
   * Fails at indices 25, 50, 75, 110.
   *
   * Provide a suitable input range to get the # of failures you want. e.g. range 0 to 100 will fail
   * 3 times.
   */
  def getFailingIterSupplier(
      range: Range,
      failIndices: Seq[Int] = Seq.empty): Supplier[CloseableIterator[String]] =
    new Supplier[CloseableIterator[String]] {
      var numGetCalls = 0

      override def get(): CloseableIterator[String] = {
        if (numGetCalls < failIndices.length) {
          val result = getIter(range, Some(failIndices(numGetCalls)))
          numGetCalls = numGetCalls + 1
          result
        } else {
          getIter(range)
        }
      }
    }

  test("simple case - internally keeps track of the correct index") {
    val testIter = new RetryableCloseableIterator(() => getIter(0 to 100))
    assert(testIter.getLastSuccessfullIndex == -1)

    for (i <- 0 to 100) {
      val elem = testIter.next()
      assert(elem.toInt == i)
      assert(testIter.getLastSuccessfullIndex == i)
    }

    assert(!testIter.hasNext) // this would be index 101
  }

  test("complex case - replays underlying iter back to correct index after error") {
    // Here, we just do the simplest verification
    val testIter1 = new RetryableCloseableIterator(
      getFailingIterSupplier(0 to 100, Seq(25, 50, 75)))

    // this asserts the size, order, and elements of the testIter1
    assert(testIter1.asScala.toList.map(_.toInt) == (0 to 100).toList)

    // Here, we do more complex verification
    val testIter2 = new RetryableCloseableIterator(
      getFailingIterSupplier(0 to 100, Seq(25, 50, 75)))

    for (_ <- 0 to 24) { testIter2.next() }
    assert(testIter2.getLastSuccessfullIndex == 24)
    assert(testIter2.getNumRetries == 0)

    assert(testIter2.next().toInt == 25) // this will fail once, and then re-scan
    assert(testIter2.getLastSuccessfullIndex == 25)
    assert(testIter2.getNumRetries == 1)

    for (_ <- 26 to 49) { testIter2.next() }
    assert(testIter2.getLastSuccessfullIndex == 49)
    assert(testIter2.getNumRetries == 1)

    assert(testIter2.next().toInt == 50) // this will fail once, and then re-scan
    assert(testIter2.getLastSuccessfullIndex == 50)
    assert(testIter2.getNumRetries == 2)

    for (_ <- 51 to 74) { testIter2.next() }
    assert(testIter2.getLastSuccessfullIndex == 74)
    assert(testIter2.getNumRetries == 2)

    assert(testIter2.next().toInt == 75) // this will fail once, and then re-scan
    assert(testIter2.getLastSuccessfullIndex == 75)
    assert(testIter2.getNumRetries == 3)

    for (_ <- 76 to 100) { testIter2.next() }
    assert(testIter2.getLastSuccessfullIndex == 100)
    assert(!testIter2.hasNext)
  }

  test("handles exceptions while retrying") {
    // Iterates normally until index 50 (return [0, 49] successfully). Then fails.
    // Tries to replay, but fails at 30
    // Tries to replay again, but fails at 20
    // Successfully replays to 49, starts returning results from index 50 (inclusive) again
    val testIter1 =
      new RetryableCloseableIterator(getFailingIterSupplier(0 to 100, Seq(50, 30, 20)))

    assert(testIter1.asScala.toList.map(_.toInt) == (0 to 100).toList)

    // Iterates normally until index 50 (return [0, 49] successfully). Then fails.
    // Successfully replayed to 49, starts returning results from index 50 (inclusive)
    // Fails at index 50 (returned [50, 69]). Tries to replay, but fails at 5
    // Successfully replayes until 69, then normally returns results from 70
    val testIter2 =
      new RetryableCloseableIterator(getFailingIterSupplier(0 to 100, Seq(50, 70, 5)))
    assert(testIter2.asScala.toList.map(_.toInt) == (0 to 100).toList)
  }

  test("throws after MAX_RETRIES exceptions") {
    val testIter =
      new RetryableCloseableIterator(getFailingIterSupplier(0 to 100, Seq(20, 49, 60, 80)))

    for (i <- 0 to 79) {
      assert(testIter.next().toInt == i)
    }
    assert(testIter.getNumRetries == 3)
    val ex = intercept[RuntimeException] {
      testIter.next()
    }
    assert(ex.getCause.isInstanceOf[RemoteFileChangedException])
  }

}
