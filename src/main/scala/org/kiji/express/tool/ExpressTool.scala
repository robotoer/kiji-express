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

package org.kiji.express.tool

import java.util.Arrays
import scala.collection.JavaConverters._

import org.apache.commons.cli.{OptionBuilder, Options}
import org.apache.hadoop.conf.Configured
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.util.GenericOptionsParser

import org.kiji.schema.tools.KijiTool
import org.kiji.delegation.Lookups

final case class ToolSchema(
    name: String,
    description: Option[String] = None,
    commands: Seq[Command] = Seq(),
    flags: Seq[Flag] = Seq()
)

final case class Command(
    name: String,
    description: Option[String] = None,
    flags: Seq[Flag] = Seq()
)

sealed trait Flag

final case class BooleanFlag(
    name: String,
    shortName: String = None,
    description: Option[String] = None
) extends Flag

final case class StringFlag(
    name: String,
    shortName: String = None,
    description: Option[String] = None
) extends Flag

final case class IntFlag(
    name: String,
    shortName: String = None,
    description: Option[String] = None
) extends Flag

object ToolParser {
  def parse(
      args: Array[String],
      schema: ToolSchema
  ): Either[String, CommandAction] = {
    null
  }
}

final case class CommandAction(
    name: String,
    flags: Seq[Any],
    args: Array[String]
)

/**
 * command(Seq[Dimension](
 *     dimension("command", Seq("create", "info", "run", "shell", "schema-shell", "help", "version")),
 *
 *
 * CliSchema(
 *     name        = "express",
 *     description = "Command line tool for the KijiExpress project.",
 *
 *     commands    = Seq(
 *         // Artifact commands.
 *         Command(
 *             name        = "create",
 *             description = "Create a new KijiExpress project.",
 *             flags       = Seq(
 *                 BooleanFlag("help", "h", "Display usage information for this tool."),
 *                 StringFlag("type", "t", "The type of the target artifact.")
 *             )
 *         ),
 *         Command(
 *             name        = "info",
 *             description = "Get information about a KijiExpress artifact.",
 *             flags       = Seq(
 *                 BooleanFlag("help", "h", "Display usage information for this tool."),
 *                 StringFlag("type", "t", "The type of the target artifact.")
 *             )
 *         ),
 *         Command(
 *             name = "run",
 *             description = "Execute a KijiExpress artifact.",
 *             flags = Seq(
 *                 BooleanFlag("help", "h", "Display usage information for this tool."),
 *                 StringFlag("type", "t", "The type of the target artifact.")
 *             )
 *         ),
 *
 *         // Interactive commands.
 *         Command(
 *             name        = "shell",
 *             description = "Launch the KijiExpress shell.",
 *         ),
 *         Command(
 *             name        = "schema-shell",
 *             description = "Launch the Kiji DDL shell with KijiExpress extensions loaded.",
 *         ),
 *
 *         // Miscellaneous commands.
 *         Command(
 *             name        = "help",
 *             description = "Print usage information for this tool."
 *         ),
 *         Command(
 *             name        = "version",
 *             description = "Print version information about this tool."
 *         )
 *     ),
 *
 *     flags        = Seq()
 * )
 *
 * CliParser.parse(args, schema) match {
 *   case Success(CommandAction("create", flags, createArgs)) => ???
 *   case Success(CommandAction("run", flags, runArgs)) => ???
 *   case Failure(reason) => sys.error(reason)
 * }
 *
 * Base implementation of a KijiExpress command line tool.
 *
 * @param name of the tool.
 * @param category of the tool.
 * @param description to be presented to the user when asking for help.
 * @param usage string to be presented to the user when asking for help.
 */
abstract class ExpressTool(
    val name: String,
    val category: String,
    val description: String,
    val usage: String = "Usage:%n    kiji %s [flags...]%n".format(name)
) extends Configured with KijiTool {
  /**
   * Main entry point for the tool.
   *
   * @param args passed to the tool.
   * @return an error code representing the success or failure of the tool.
   */
  def run(args: Array[String]): Int

  override def getName: String = name
  override def getCategory: String = category
  override def getDescription: String = description
  override def getUsageString: String = usage

  override def toolMain(args: java.util.List[String]): Int = run(args.toArray(Array[String]()))
}

object ExpressTool {
  def flagToOption(flag: Flag): org.apache.commons.cli.Option = {
    flag match {
      case BooleanFlag(name, shortName, description) => {
        option(
          shortName = shortName,
          longName = Some(name),
          description = description
        )
      }
      case StringFlag(name, shortName, description) => {
        option(
          shortName = shortName,
          longName = Some(name),
          description = description,
          args = 1,
          argName = name
        )
      }
      case IntFlag(name, shortName, description) => {
        option(
          shortName = shortName,
          longName = Some(name),
          description = description,
          args = 1,
          argName = name
        )
      }
    }
  }

  def toolMain(args: Array[String], schema: ToolSchema): Int = {
    val flags = schema
        .flags
        .map { flagToOption }

    val commandFlags = schema
        .commands
        .flatMap { command: Command =>
          command
              .flags
              .map { flag => (flagToOption(flag), command) }
        }
        .toMap

    // Build command line arguments.
    val options = flags
        .foldLeft(new Options()) { (accumulator, option) => accumulator.addOption(option) }
    val commandOptions = commandFlags
        .keys
        .foldLeft(new Options()) { (accumulator, option) => accumulator.addOption(option) }

    // Work around for CDH4 and Hadoop1 setting different "GenericOptionsParser used" flags.
    val configuration = HBaseConfiguration.create()
    configuration.setBooleanIfUnset("mapred.used.genericoptionsparser", true)
    configuration.setBooleanIfUnset("mapreduce.client.genericoptionsparser.used", true)

    // Parse command line arguments.
    val parser = new GenericOptionsParser(configuration, options, args)

    parser.getRemainingArgs match {
      case Array(toolName: String, toolArgs @ _*) => {
        val expressTool = Lookups
            .get(classOf[ExpressTool])
            .asScala
            .find { _.getName == toolName }

        expressTool match {
          case Some(tool) => {
            tool.setConf(parser.getConfiguration)
            tool.toolMain(Arrays.asList(toolArgs: _*))

            System.exit(0)
          }
          case None =>  {
            System.err.println("Error: Must run 'kiji <toolName>'.")
            System.err.println("Try running 'kiji help' to see the available tools.")

            System.exit(1)
          }
        }
      }
    }

    return 0
  }

  val expressSchema: ToolSchema = ToolSchema(
      name        = "express",
      description = Some("Command line tool for the KijiExpress project."),

      commands    = Seq(
          // Artifact commands.
          Command(
              name        = "create",
              description = Some("Create a new KijiExpress project."),
              flags       = Seq(
                  BooleanFlag("help", "h", Some("Display usage information for this tool.")),
                  StringFlag("type", "t", Some("The type of the target artifact."))
              )
          ),
          Command(
              name        = "info",
              description = Some("Get information about a KijiExpress artifact."),
              flags       = Seq(
                  BooleanFlag("help", "h", Some("Display usage information for this tool.")),
                  StringFlag("type", "t", Some("The type of the target artifact."))
              )
          ),
          Command(
              name = "run",
              description = Some("Execute a KijiExpress artifact."),
              flags = Seq(
                  BooleanFlag("help", "h", Some("Display usage information for this tool.")),
                  StringFlag("type", "t", Some("The type of the target artifact."))
              )
          ),

          // Interactive commands.
          Command(
              name        = "shell",
              description = Some("Launch the KijiExpress shell.")
          ),
          Command(
              name        = "schema-shell",
              description = Some("Launch the Kiji DDL shell with KijiExpress extensions loaded.")
          ),

          // Miscellaneous commands.
          Command(
              name        = "help",
              description = Some("Print usage information for this tool.")
          ),
          Command(
              name        = "version",
              description = Some("Print version information about this tool.")
          )
      ),

      flags        = Seq()
  )






  def option(
      shortName: String,
      longName: Option[String] = None,
      description: Option[String] = None,
      required: Boolean = false,
      args: Int = 0,
      optionalArgs: Int = 0,
      argName: String = "arg",
      optionType: AnyRef = null,
      argSeparator: Char = '\0'): org.apache.commons.cli.Option = {
    // TODO: Replace this with the option constructor.
    // This builder is not thread-safe.
    synchronized {
      longName.foreach { OptionBuilder.withLongOpt }
      description.foreach { OptionBuilder.withDescription }
      OptionBuilder.isRequired(required)
      OptionBuilder.hasArgs(args)
      OptionBuilder.hasOptionalArgs(optionalArgs)
      OptionBuilder.withArgName(argName)
      OptionBuilder.withType(optionType)
      OptionBuilder.withValueSeparator(argSeparator)

      OptionBuilder.create(shortName)
    }
  }

  def main(args: Array[String]) {
    // Build command line arguments.
    val options = new Options()
        .addOption("h", "help", false, "Print the usage message.")

    // Work around for CDH4 and Hadoop1 setting different "GenericOptionsParser used" flags.
    val configuration = HBaseConfiguration.create()
    configuration.setBooleanIfUnset("mapred.used.genericoptionsparser", true)
    configuration.setBooleanIfUnset("mapreduce.client.genericoptionsparser.used", true)

    // Parse command line arguments.
    val parser = new GenericOptionsParser(configuration, options, args)

    parser.getRemainingArgs match {
      case Array(toolName: String, toolArgs @ _*) => {
        val expressTool = Lookups
            .get(classOf[ExpressTool])
            .asScala
            .find { _.getName == toolName }

        expressTool match {
          case Some(tool) => {
            tool.setConf(parser.getConfiguration)
            tool.toolMain(Arrays.asList(toolArgs: _*))

            System.exit(0)
          }
          case None =>  {
            System.err.println("Error: Must run 'kiji <toolName>'.")
            System.err.println("Try running 'kiji help' to see the available tools.")

            System.exit(1)
          }
        }
      }
    }
  }
}