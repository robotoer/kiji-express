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

package org.kiji.express.modeling.lib

import scala.collection.JavaConverters._

import cascading.tuple.Fields

import org.kiji.express.modeling.{Scorer, ScoreFn}
import org.dmg.pmml.{TransformationDictionary, MiningBuildTask, Header, Extension, Model, DataDictionary, PMML}
import org.jpmml.manager.{ModelManagerFactory, PMMLManager}

class PmmlScorer extends Scorer {
  override def scoreFn: ScoreFn[_, _] = score(Fields.ALL) { tuple: Product =>
    // Load the pmml model and store it.
    val pmml: PMML = keyValueStore[String, PMML]("pmml")
        .apply("model")
    val dataDict: DataDictionary = pmml.getDataDictionary
    dataDict.getDataFields
    dataDict.getExtensions
    dataDict.getNumberOfFields
    dataDict.getTaxonomies
    val content: Seq[Model] = pmml.getContent.asScala
    val extensions: Seq[Extension] = pmml.getExtensions.asScala
    val header: Header = pmml.getHeader
    val miningBuildTask: MiningBuildTask = pmml.getMiningBuildTask
    val transformationDictionary: TransformationDictionary = pmml.getTransformationDictionary
    val version: String = pmml.getVersion

    // How do you get the field names/how do you assign parameters to the Pmml model?
    val pmmlManager: PMMLManager = new PMMLManager(pmml)
    val modelManager = pmmlManager.getModelManager(null, ModelManagerFactory.getInstance())

    // Find out what fields the pmml model is expecting and build a seq of them.

    // Unpack the incoming tuple into the desired arguments and build a map from field name to
    // value.

    val tupleElements = tuple
        .productIterator

    // Evaluate the model using jpmml.

    // Store the resulting output in the output column.
  }
}
