/**
 * (c) Copyright 2014 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.express.flow

import com.twitter.scalding.Args
import com.twitter.scalding.JobTest
import com.twitter.scalding.Source
import com.twitter.scalding.Tsv
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import org.kiji.express.KijiSuite

@RunWith(classOf[JUnitRunner])
class TupleProfilingSuite extends KijiSuite {
  test("Test even binning") {
    // Creates 4 bins:
    //  - test-even-histogram0: (-infinity, 0.0)
    //  - test-even-histogram1: [0.0, 10.0)
    //  - test-even-histogram2: [10.0, 20.0)
    //  - test-even-histogram3: [20.0, infinity)
    val equalWidthBinConfig: (Double) => Int = TupleProfiling.equalWidthBinConfig(0.0, 10.0, 2)

    assert(0 == equalWidthBinConfig(-15.0))
    assert(0 == equalWidthBinConfig(-5.0))

    assert(1 == equalWidthBinConfig(0.0))
    assert(1 == equalWidthBinConfig(5.0))

    assert(2 == equalWidthBinConfig(10.0))
    assert(2 == equalWidthBinConfig(11.0))

    assert(3 == equalWidthBinConfig(20.0))
    assert(3 == equalWidthBinConfig(30.0))
  }

  test("Test logarithmic binning") {
    // Creates 5 bins:
    //  - test-log-histogram0: [0.0, 1.0)
    //  - test-log-histogram1: [1.0, 10.0)
    //  - test-log-histogram2: [10.0, 100.0)
    //  - test-log-histogram3: [100.0, 1000.0)
    //  - test-log-histogram4: [1000.0, infinity)
    val logWidthBinConfig: (Double) => Int = TupleProfiling.logWidthBinConfig(3, 10.0, 0.0, 1.0)

    val expectedException = try {
      logWidthBinConfig(0.0)

      None
    }

    assert(0 == logWidthBinConfig(0.5))
    assert(0 == logWidthBinConfig(0.9))

    assert(1 == logWidthBinConfig(1.0))
    assert(1 == logWidthBinConfig(5.0))

    assert(2 == logWidthBinConfig(10.0))
    assert(2 == logWidthBinConfig(50.0))

    assert(3 == logWidthBinConfig(100.0))
    assert(3 == logWidthBinConfig(500.0))

    assert(3 == logWidthBinConfig(1000.0))
    assert(3 == logWidthBinConfig(15000.0))
  }

  test("Test tuple size profiling") {
    val data: Seq[Array[Int]] = Seq(
        Array(1),
        Array(1, 2)
    )
    new JobTest(new TupleProfilingSuite.SizeProfilingJob(_))
        .arg("input", "input")
        .arg("output", "output")
        .arg("size-histogram", "")
        .source(Tsv("input"), data)
        .sink[Array[Int]](Tsv("output"))({ output: Seq[Array[Int]] => data == output })
        .run
        .runHadoop
        .counter(
            "SIZE_OF_TUPLES4",
            TupleProfiling.HISTOGRAM_COUNTER_GROUP
        )({ counter: Long => assert(counter == 1L) })
        .counter(
            "SIZE_OF_TUPLES8",
            TupleProfiling.HISTOGRAM_COUNTER_GROUP
        )({ counter: Long => assert(counter == 1L) })
        .finish
  }

  test("Test tuple time profiling") {
    // Number of milliseconds to sleep in the map phase.
    val data: Seq[Int] = Seq(10, 100, 1000)
    new JobTest(new TupleProfilingSuite.TimeProfilingJob(_))
        .arg("input", "input")
        .arg("output", "output")
        .arg("time-histogram", "")
        .source(Tsv("input"), data)
        .sink[Int](Tsv("output"))({ (output: Seq[Int]) => data == output })
        .run
        .runHadoop
        .counter(
            "TIME_TO_PROCESS_TUPLES0",
            TupleProfiling.HISTOGRAM_COUNTER_GROUP
        )({ counter: Long => assert(counter == 1L) })
        .counter(
            "TIME_TO_PROCESS_TUPLES1",
            TupleProfiling.HISTOGRAM_COUNTER_GROUP
        )({ counter: Long => assert(counter == 1L) })
        .counter(
            "TIME_TO_PROCESS_TUPLES2",
            TupleProfiling.HISTOGRAM_COUNTER_GROUP
        )({ counter: Long => assert(counter == 1L) })
        .finish
  }
}

object TupleProfilingSuite {
  /**
   * Example job for profiling tuples by size. Will profile the size of the first element of tuples.
   *
   * @param args to the job. These get parsed in from the command line by Scalding.
   */
  class SizeProfilingJob(args: Args) extends KijiJob(args) {
    def inputSource: Source = Tsv(args("input"))
    def outputSource: Source = Tsv(args("output"))

    private val sizeHistogram: HistogramConfig =
        new HistogramConfig(
            mName = "SIZE_OF_TUPLES",
            mPath = args("size-histogram"),
            mBinner = TupleProfiling.equalWidthBinConfig(
                binStart = 0.0,
                binSize = 1.0,
                binCount = 10
            )
        )

    inputSource
        .map(0 -> 'newData) { original: Array[Int] =>
          TupleProfiling.profileSize(sizeHistogram, original)

          original
        }
        .write(outputSource)
  }

  /**
   * Example job for profiling function processing times. Will sleep for the int number of
   * milliseconds from the first element of the tuple.
   *
   * @param args to the job. These get parsed in from the command line by Scalding.
   */
  class TimeProfilingJob(args: Args) extends KijiJob(args) {
    def inputSource: Source = Tsv(args("input"))
    def outputSource: Source = Tsv(args("output"))

    private val timeHistogram: HistogramConfig =
        new HistogramConfig(
            mName = "TIME_TO_PROCESS_TUPLES",
            mPath = args("time-histogram"),
            mBinner = TupleProfiling.logWidthBinConfig(binCount = 3, logBase = 10.0)
        )

    inputSource
        .map(0 -> 'newData) { original: Int =>
          TupleProfiling.profileTime(
              timeHistogram,
              (input: Int) => {
                Thread.sleep(input)
                input
              }
          )
        }
        .write(outputSource)
  }
}
