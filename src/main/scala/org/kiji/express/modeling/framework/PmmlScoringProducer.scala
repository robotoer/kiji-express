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

import org.apache.hadoop.conf.Configuration

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.express.modeling.config.ModelDefinition
import org.kiji.express.modeling.config.ModelEnvironment
import org.kiji.express.modeling.impl.ModelJobUtils
import org.kiji.express.modeling.impl.ModelJobUtils.PhaseType.SCORE
import org.kiji.mapreduce.produce.KijiProducer
import org.kiji.mapreduce.produce.ProducerContext
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.KijiRowData

@ApiAudience.Framework
@ApiStability.Experimental
final class PmmlScoringProducer
    extends KijiProducer
    with ModelConfigurable {
  override def setConf(conf: Configuration) {
    super.setConf(conf)

    // Ensure that a pmml model is defined and no other phases are defined.

    // Load the pmml model and store it in a var.

    // Setup any other jpmml objects as necessary.
  }

  override def getDataRequest: KijiDataRequest = ModelJobUtils
      .getDataRequest(modelEnvironment, SCORE)
      .get

  override def getOutputColumn: String = ModelJobUtils
      .getOutputColumn(modelEnvironment)

  override def produce(input: KijiRowData, context: ProducerContext) {
    // Find out what fields the pmml model is expecting and build a seq of them.

    // Unpack the incoming tuple into the desired arguments and build a map from field name to
    // value.

    // Evaluate the model using jpmml.

    // Store the resulting output in the output column.
  }
}
