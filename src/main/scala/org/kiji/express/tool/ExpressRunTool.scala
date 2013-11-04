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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.express.tool

import org.apache.commons.cli._

import org.kiji.schema.tools.KijiToolLauncher
import org.apache.hadoop.util.GenericOptionsParser
import scala.Some

// TODO: Rename "job" -> "flow".
// Really should be:
// express create <target> [--type=<job>]
// express info <target> [--type=<script, job, jar>]
// express run <target> [--type=<script, job, jar>]
// express shell
// express schema-shell
// express help
// express version
// Add support for bash tab completion.

// General concepts around command line parsing
// - Shape of the command
// - Argument validation
// - Attaching callbacks to various command points?
/**
 * Command line tool for executing a KijiExpress artifact:
 *
 * express run &lt;target&gt; [--type=&lt;script, job, jar&gt;]
 */
final class ExpressRunTool
    extends ExpressTool("run", "basic", "Run a KijiExpress artifact (script, job, or jar).") {
  def help: String = ""

  override def run(args: Array[String]): Int = {
    // Create command line options.
    val helpOption = ExpressTool.option(
        shortName = "h",
        longName = Some("help"),
        description = Some("Print the usage message."))
    val typeOption = ExpressTool.option(
        shortName = "t",
        longName = Some("type"),
        description = Some("The type of the specified artifact."),
        argName = "<script, job, or jar>",
        args = 1)
    val options: Options = new Options()
        .addOption(helpOption)
        .addOption(typeOption)

    // Parse the command line arguments.
    val parser: CommandLineParser = new GnuParser()
    val commandLine = parser.parse(options, args, true)
//    val commandLine = try {
//      parser.parse(options, args, true)
//    } catch {
//      case pe: ParseException => {
//        // Print help message.
//        Console.err.println()
//      }
//    }

    // Run the artifact.
    if (commandLine.hasOption("h")) {
      Console.err.println(help)

      return 0
    } else {
      if (commandLine.hasOption("t")) {

      }
    }
  }
}

object ExpressRunTool {
  /**
   * Program entry point.
   *
   * @param args The command-line arguments.
   */
  def main(args: Array[String]) {
    System.exit(new KijiToolLauncher().run(new ExpressRunTool, args))
  }
}
