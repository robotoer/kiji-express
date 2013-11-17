/**
 * (c) Copyright 2013 WibiData, Inc.
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

package org.kiji.express.flow.framework.hfile

import scala.transient

import cascading.flow.Flow
import cascading.tap.hadoop.Hfs
import cascading.util.Util
import com.twitter.scalding.Args
import com.twitter.scalding.HadoopTest
import com.twitter.scalding.Hdfs
import com.twitter.scalding.Job
import com.twitter.scalding.Mode
import com.twitter.scalding.WritableSequenceFile
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.JobConf

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.express.flow.KijiJob
import org.kiji.mapreduce.framework.HFileKeyValue

/**
 * HFileKijiJob is an extension of KijiJob and users should extend it instead of KijiJob when
 * writing jobs in KijiExpress that need to output HFiles that can eventually be bulk-loaded into
 * HBase.
 *
 * In your HFileKijiJob, you need to write to a source constructed with
 * [[org.kiji.express.flow.framework.hfile.HFileKijiOutput]].
 *
 * You can extend HFileKijiJob like this:
 *
 * {{{
 * class MyKijiExpressClass(args) extends HFileKijiJob(args) {
 *   // Your code here.
 *   .write(HFileKijiOutput(tableUri = "kiji://localhost:2181/default/mytable",
 *       hFileOutput = "my_hfiles",
 *       timestampField = 'timestamps,
 *       'column1 -> "info:column1",
 *       'column2 -> "info:column2"))
 * }
 * }}}
 *
 *     NOTE: To properly work with dumping to HFiles, the argument --hfile-output must be provided.
 *     This argument specifies the location where the HFiles will be written upon job completion.
 *     Also required is the --output flag. This argument specifies the Kiji table to use to obtain
 *     layout information to properly format the HFiles for bulk loading.
 *
 * @param args to the job. These get parsed in from the command line by Scalding.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Extensible
class HFileKijiJob(args: Args) extends KijiJob(args) {
  /** Name of the command-line argument that specifies the temporary HFile root directory. */
  final val HFileOutputArgName: String = "hfile-output"

  /** Name of the command-line argument that specifies the target output table. */
  final val TableOutputArgName: String = "output"

  // Force the check to ensure that a value has been provided for the hFileOutput
  args(HFileOutputArgName)
  args(TableOutputArgName)

  @transient
  lazy private val jobConf: Configuration = implicitly[Mode] match {
    case Hdfs(_, configuration) => {
      configuration
    }
    case HadoopTest(configuration, _) => {
      configuration
    }
    case _ => new JobConf()
  }

  @transient
  lazy val uniqTempFolder = makeTemporaryPathDirString("HFileDumper")

  val tempPath = new Path(Hfs.getTempPath(jobConf.asInstanceOf[JobConf]), uniqTempFolder).toString

  override def config(implicit mode: Mode): Map[AnyRef, AnyRef] = {
    val baseConfig = super.config(mode)
    baseConfig ++ Map(HFileKijiOutput.TEMP_HFILE_OUTPUT_KEY -> tempPath.toString())
  }

  override def buildFlow(implicit mode: Mode): Flow[_] = {
    val flow = super.buildFlow
    // Here we set the strategy to change the sink steps since we are dumping to HFiles.
    flow.setFlowStepStrategy(new HFileFlowStepStrategy)
    flow
  }

  override def next: Option[Job] = {
    val fs = FileSystem.get(jobConf)
    if(fs.exists(new Path(tempPath))) {
      val newArgs = args + ("input" -> Some(tempPath))
      val job = new HFileMapJob(newArgs)
      Some(job)
    } else {
      None
    }
  }

  // Borrowed from Hfs#makeTemporaryPathDirString
  private def makeTemporaryPathDirString(name: String) = {
    // _ is treated as a hidden file, so wipe them out
    val name2 = name.replaceAll("^[_\\W\\s]+", "")

    val name3 = if (name2.isEmpty()) {
      "temp-path"
    } else {
      name2
    }

    name3.replaceAll("[\\W\\s]+", "_") + Util.createUniqueID()
  }
}

/**
 * Private job implementation that executes the conversion of the intermediary HFile key-value
 * sequence files to the final HFiles. This is done only if the first job had a Cascading
 * configured reducer.
 */
@ApiAudience.Private
@ApiStability.Experimental
private final class HFileMapJob(args: Args) extends HFileKijiJob(args) {

  override def next: Option[Job] = {
    val conf: Configuration = implicitly[Mode] match {
      case Hdfs(_, configuration) => {
        configuration
      }
      case HadoopTest(configuration, _) => {
        configuration
      }
      case _ => new JobConf()
    }
    val fs = FileSystem.get(conf)
    val input = args("input")
    fs.delete(new Path(input), true)
    None
  }

  WritableSequenceFile[HFileKeyValue, NullWritable](args("input"), ('keyValue, 'bogus))
      .write(new HFileSource(args(TableOutputArgName),args(HFileOutputArgName)))
}
