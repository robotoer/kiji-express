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

import java.util.Properties

import scala.collection.JavaConverters.mapAsScalaMapConverter

import cascading.flow.FlowProcess
import cascading.kryo.KryoSerialization
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Output
import com.google.common.base.Preconditions
import com.twitter.scalding.RuntimeStats
import com.twitter.scalding.UniqueID
import org.apache.hadoop.conf.Configuration

/**
 * Provides support for generating profiling histograms. Currently 2 types of histograms are
 * supported:
 * <ul>
 *   <li>sizes of tuples passing through a point in a scalding job</li>
 *   <li>time to complete an operation in a scalding job</li>
 * </ul>
 *
 * Size based profiling:
 * <ul>
 *   <li>Produces tuple size histogram for tuples at a specific point in the job.</li>
 *   <li>
 *     Requires that input/output data is serializable by kryo (already an assumption that scalding
 *     makes).
 *   </li>
 *   <li>Histogram bin size unit will be in bytes.</li>
 * </ul>
 *
 * Time based profiling:
 * <ul>
 *   <li>Produces a histogram of the processing time of the operation per input.</li>
 *   <li>Histogram bin size unit will be in milliseconds.</li>
 * </ul>
 *
 * The produced histograms will use the following naming scheme for their counters:
 *   histogram-name1
 *
 * Histograms will be written to disk at the specified location. /dev/stdout, /dev/stderr can be
 * used to redirect output to the terminal.
 *
 *
 * Example:
 * <pre><code>
 * class TupleProfilingExampleJob(args: Args) extends KijiJob(args) {
 *   // Define some dummy sources.
 *   def inputSource: Source = ???
 *   def outputSource: Source = ???
 *
 *   // Create some dummy processing methods.
 *   def processData[I, O](input: I): O = ???
 *
 *   // Define the histograms to use.
 *   val sizeHistogram: HistogramConfig =
 *       HistogramConfig(
 *           name="size-of-tuples-map-1",
 *           path=args("before-size-histogram"),
 *           binConfig=TupleProfiling.equalWidthBinConfig(binSize=10, binCount=500)
 *       )
 *   val timeHistogram: HistogramConfig =
 *       HistogramConfig(
 *           name="time-to-process-map-1",
 *           path=args("time-histogram"),
 *           binConfig=TupleProfiling.logWidthBinConfig(logBase=math.E, binCount=100)
 *       )
 *
 *   inputSource
 *       // Measure the size of tuples in a map.
 *       .map('some_input_field, 'some_output_field)({ input =>
 *         TupleProfiling.profileSize(sizeHistogram, input)
 *
 *         // Can do additional processing here and can call profileSize again in here to generate
 *         // another histogram.
 *         processData(input)
 *       })
 *       // Measure the time to filter tuples.
 *       .filter('some_output_field)(
 *           TupleProfiling.profileTime(timeHistogram, processData)
 *       )
 *       .write(outputSource)
 * }
 * </code></pre>
 */
object TupleProfiling {
  val HISTOGRAM_COUNTER_GROUP: String = "Express Histogram Counters"

  /**
   * Profiles the sizes of the provided tuples. Requires that the provided data is serializable by
   * Kryo.
   *
   * @tparam T is the type of the tuple.
   * @param histogramConfig defining the histogram (bin settings and name).
   * @param tuple to profile.
   * @param uniqueIdCont used to identify the job that this profile method is being used within.
   *     This is used to get a kryo configured as it would be for cascading.
   * @return the provided tuple.
   */
  def profileSize[T]
      (histogramConfig: HistogramConfig, tuple: T)
      (implicit uniqueIdCont: UniqueID): T = {
    // Serialize the tuple using kryo.
    val flowProcess: FlowProcess[_] = RuntimeStats.getFlowProcessForUniqueId(uniqueIdCont.get)
    val kryo: Kryo = flowProcess.getConfigCopy match {
      case hadoopConfiguration: Configuration => {
        new KryoSerialization(hadoopConfiguration).populatedKryo()
      }
      case localConfiguration: Properties => {
        val kryoConfiguration: Configuration = localConfiguration
            .asScala
            .foldLeft(new Configuration()) { (conf: Configuration, entry: (AnyRef, AnyRef)) =>
              val (key, value) = entry
              conf.set(key.toString, value.toString)
              conf
            }
        new KryoSerialization(kryoConfiguration).populatedKryo()
      }
    }
    val output: Output = new Output(4096, Int.MaxValue)
    val tupleSize: Double = try {
      kryo.writeClassAndObject(output, tuple)
      output.total()
    } finally {
      output.close()
    }

    // Bucket the size of the byte array into the provided histogram bins.
    histogramConfig.incrementBinCount(tupleSize)(uniqueIdCont)

    tuple
  }

  /**
   * Profiles the processing time for the provided function.
   *
   * @tparam I is the input type of the function.
   * @tparam O is the output type of the function.
   * @param histogramConfig defining the histogram (bin settings and name).
   * @param fn to profile.
   * @return a function that will run and time the provided function.
   */
  def profileTime[I, O]
      (histogramConfig: HistogramConfig, fn: I => O)
      (implicit uniqueIdCont: UniqueID): I => O = {
    // Return a function that adds timing to the provided function.
    { input: I =>
      // Run the function recording the start and end timestamps.
      val startTime: Long = System.nanoTime()
      val returnValue: O = fn(input)
      val stopTime: Long = System.nanoTime()
      val elapsedTime = stopTime - startTime

      // Bucket the time to process into the provided histogram bins.
      histogramConfig.incrementBinCount(elapsedTime)(uniqueIdCont)

      returnValue
    }
  }

  /**
   * Sets up a log-width binning configuration. The width of bins will increase exponentially with
   * increasing bin IDs.
   *
   * @param binCount is the total number of bins to create.
   * @param logBase is the exponential base to expand the width of bins with.
   * @param startingPower is the starting power to raise the exponential base to.
   * @param powerStepSize is the amount to increase the power the exponential base is raised to for
   *     each bin.
   * @return a binning function for this log-width binning configuration.
   */
  def logWidthBinConfig(
      binCount: Int,
      logBase: Double = math.E,
      startingPower: Double = 0.0,
      powerStepSize: Double = 1.0
  ): Double => Int = {
    val binMapping = new java.util.TreeMap[Double, Int]()
    for (i <- 0.until(binCount)) {
      binMapping.put(math.pow(logBase, i * powerStepSize + startingPower), i)
    }

    // Return a function.
    { stat: Double =>
      Preconditions.checkState(
          stat > 0.0,
          "Expected stat to be larger than 0: %f",
          stat: java.lang.Double
      )

      math.floor((math.log(stat) / math.log(logBase) - startingPower) / powerStepSize).toInt
    }
  }

  /**
   * Sets up an equal-width binning configuration. The width of bins will be constant.
   *
   * @param binStart is the upper bound (exclusive) of the first bin.
   * @param binSize is the width of each bin.
   * @param binCount is the total number of bins to create.
   * @return a binning function for this equal-width binning configuration.
   */
  def equalWidthBinConfig(binStart: Double, binSize: Double, binCount: Int): Double => Int = {
    // Return a function.
    { stat: Double =>
      val rawBinId: Int = math.floor((stat - binStart) / binSize).toInt + 1
      math.min(binCount + 1, math.max(rawBinId, 0))
    }
  }
}
