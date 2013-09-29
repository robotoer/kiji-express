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

package org.kiji.express.modeling.framework

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.express.KijiSlice
import org.kiji.express.modeling.ExtractFn
import org.kiji.express.modeling.Extractor
import org.kiji.express.modeling.config.KijiInputSpec
import org.kiji.express.modeling.config.KeyValueStoreSpec
import org.kiji.express.modeling.impl.ModelJobUtils
import org.kiji.express.modeling.impl.ModelJobUtils.PhaseType.SCORE
import org.kiji.express.modeling.ScoreFn
import org.kiji.express.modeling.Scorer
import org.kiji.express.util.GenericRowDataConverter
import org.kiji.express.util.Tuples
import org.kiji.mapreduce.KijiContext
import org.kiji.mapreduce.kvstore.{ KeyValueStore => JKeyValueStore }
import org.kiji.mapreduce.produce.KijiProducer
import org.kiji.mapreduce.produce.ProducerContext
import org.kiji.schema.KijiColumnName
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.KijiRowData
import org.kiji.schema.KijiURI

/**
 * A producer for running [[org.kiji.express.modeling.config.ModelDefinition]]s.
 *
 * This producer executes the score phase of a model. The model that this producer will run is
 * loaded from the json configuration strings stored in configuration keys:
 * <ul>
 *   <li>`org.kiji.express.model.definition`</li>
 *   <li>`org.kiji.express.model.environment`</li>
 * </ul>
 */
@ApiAudience.Framework
@ApiStability.Experimental
final class ScoringProducer
    extends KijiProducer
    with ModelConfigurable {
  /** Extractor to use for this model definition. This variable must be initialized. */
  private var _extractor: Option[Extractor] = None
  private def extractor: Option[Extractor] = _extractor

  /** Scorer to use for this model definition. This variable must be initialized. */
  private var _scorer: Option[Scorer] = None
  private def scorer: Scorer = {
    _scorer.getOrElse {
      throw new IllegalStateException(
          "ScoreProducer is missing its scorer. Did setConf get called?")
    }
  }

  /** A converter that configured row data to decode data generically. */
  private var _rowConverter: Option[GenericRowDataConverter] = None
  private def rowConverter: GenericRowDataConverter = {
    _rowConverter.getOrElse {
      throw new IllegalStateException("ExtractScoreProducer is missing its row data converter. "
          + "Was setup() called?")
    }
  }

  /**
   * Sets the Configuration for this KijiProducer to use. This function is guaranteed to be called
   * immediately after instantiation.
   *
   * This method loads a [[org.kiji.express.modeling.config.ModelDefinition]] and a
   * [[org.kiji.express.modeling.config.ModelEnvironment]] for ScoreProducer to use.
   *
   * @param conf object that this producer should use.
   */
  override def setConf(conf: Configuration) {
    super.setConf(conf)

    // Make an instance of each requires phase.
    _extractor = modelDefinition
        .scoreExtractorClass
        .map { _.newInstance() }
    _scorer = modelDefinition
        .scorerClass
        .map { _.newInstance() }

    require(_scorer.isDefined, "A scorer was not specified!")
  }

  /**
   * Returns a KijiDataRequest that describes which input columns need to be available to the
   * producer. This method may be called multiple times, perhaps before `setup`.
   *
   * This method reads the Extract phase's data request configuration from this model's run profile
   * and builds a KijiDataRequest from it.
   *
   * @return a kiji data request.
   */
  override def getDataRequest: KijiDataRequest = ModelJobUtils
      .getDataRequest(modelEnvironment, SCORE)
      .get

  /**
   * Returns the name of the column this producer will write to.
   *
   * This method reads the Score phase's output column from this model's run profile and returns it.
   *
   * @return the output column name.
   */
  override def getOutputColumn: String = ModelJobUtils
      .getOutputColumn(modelEnvironment)

  /**
   * Opens the key value stores required for the extract and score phase. Reads key value store spec
   * configurations from the provided model environment.
   *
   * @return a mapping from keyValueStoreSpec names to opened key value stores.
   */
  override def getRequiredStores: java.util.Map[String, JKeyValueStore[_, _]] = {
    // Open the key value stores defined for the score phase.
    val scoreStoreDefs: Seq[KeyValueStoreSpec] = modelEnvironment
        .scoreEnvironment
        .get
        .keyValueStoreSpecs
    val scoreStores: Map[String, JKeyValueStore[_, _]] = ModelJobUtils
        .openJKvstores(scoreStoreDefs, getConf)
    scoreStores.asJava
  }

  override def cleanup(context: KijiContext) {
    rowConverter.close()
  }

  override def setup(context: KijiContext) {
    // Setup the score phase's key value stores.
    val scoreStoreDefs: Seq[KeyValueStoreSpec] = modelEnvironment
        .scoreEnvironment
        .get
        .keyValueStoreSpecs
    scorer.keyValueStores = ModelJobUtils
        .wrapKvstoreReaders(scoreStoreDefs, context)
    extractor.foreach { _.keyValueStores = scorer.keyValueStores }

    // Setup the row converter.
    val uriString = modelEnvironment
        .scoreEnvironment
        .get
        .inputSpec
        .asInstanceOf[KijiInputSpec]
        .tableUri
    val uri = KijiURI.newBuilder(uriString).build()
    _rowConverter = Some(new GenericRowDataConverter(uri, getConf))
  }

  override def produce(input: KijiRowData, context: ProducerContext) {
    val ExtractFn(extractFields, extract) = extractor.get.extractFn
    val ScoreFn(scoreFields, score) = scorer.scoreFn

    // Setup fields.
    val fieldMapping: Map[String, KijiColumnName] = modelEnvironment
        .scoreEnvironment
        .get
        .inputSpec
        .asInstanceOf[KijiInputSpec]
        .fieldBindings
        .map { binding =>
          (binding.tupleFieldName, new KijiColumnName(binding.storeFieldName))
        }
        .toMap
    val extractInputFields: Seq[String] = {
      // If the field specified is the wildcard field, use all columns referenced in this model
      // environment's field bindings.
      if (extractFields._1.isAll) {
        fieldMapping.keys.toSeq
      } else {
        Tuples.fieldsToSeq(extractFields._1)
      }
    }
    val extractOutputFields: Seq[String] = {
      // If the field specified is the results field, use all input fields from the extract phase.
      if (extractFields._2.isResults) {
        extractInputFields
      } else {
        Tuples.fieldsToSeq(extractFields._2)
      }
    }
    val scoreInputFields: Seq[String] = {
      // If the field specified is the wildcard field, use all fields output by the extract phase.
      if (scoreFields.isAll) {
        extractOutputFields
      } else {
        Tuples.fieldsToSeq(scoreFields)
      }
    }

    // Configure the row data input to decode its data generically.
    val row = rowConverter(input)
    // Prepare input to the extract phase.
    val slices: Seq[KijiSlice[Any]] = extractInputFields
        .map { (field: String) =>
          val columnName: KijiColumnName = fieldMapping(field.toString)

          // Build a slice from each column within the row.
          if (columnName.isFullyQualified) {
            KijiSlice[Any](row, columnName.getFamily, columnName.getQualifier)
          } else {
            KijiSlice[Any](row, columnName.getFamily)
          }
        }

    // Get output from the extract phase.
    val featureVector: Product = Tuples.fnResultToTuple(
        extract(Tuples.tupleToFnArg(Tuples.seqToTuple(slices))))
    val featureMapping: Map[String, Any] = extractOutputFields
        .zip(featureVector.productIterator.toIterable)
        .toMap

    // Get a score from the score phase.
    val scoreInput: Seq[Any] = scoreInputFields
        .map { field => featureMapping(field) }
    val scoreValue: Any =
        score(Tuples.tupleToFnArg(Tuples.seqToTuple(scoreInput)))

    // Write the score out using the provided context.
    context.put(scoreValue)
  }
}

object ScoringProducer {
  /**
   * Configuration key addressing the JSON description of a
   * [[org.kiji.express.modeling.config.ModelDefinition]].
   */
  val modelDefinitionConfKey: String = "org.kiji.express.model.definition"

  /**
   * Configuration key addressing the JSON configuration of a
   * [[org.kiji.express.modeling.config.ModelEnvironment]].
   */
  val modelEnvironmentConfKey: String = "org.kiji.express.model.environment"
}
