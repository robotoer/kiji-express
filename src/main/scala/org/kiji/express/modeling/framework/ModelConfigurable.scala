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

import org.apache.hadoop.conf.Configurable
import org.apache.hadoop.conf.Configuration

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.express.modeling.config.ModelDefinition
import org.kiji.express.modeling.config.ModelEnvironment

/**
 * A producer for running [[org.kiji.express.modeling.config.ModelDefinition]]s.
 *
 * The model that this producer will run is loaded from the json configuration strings stored in
 * configuration keys:
 * <ul>
 *   <li>`org.kiji.express.model.definition`</li>
 *   <li>`org.kiji.express.model.environment`</li>
 * </ul>
 */
@ApiAudience.Framework
@ApiStability.Experimental
trait ModelConfigurable
    extends Configurable {
  /** The model definition. This variable must be initialized. */
  private var _modelDefinition: Option[ModelDefinition] = None
  def modelDefinition: ModelDefinition = {
    _modelDefinition.getOrElse {
      throw new IllegalStateException(
          "ScoreProducer is missing its model definition. Did setConf get called?")
    }
  }

  /** Environment required to run phases of a model. This variable must be initialized. */
  private var _modelEnvironment: Option[ModelEnvironment] = None
  def modelEnvironment: ModelEnvironment = {
    _modelEnvironment.getOrElse {
      throw new IllegalStateException(
          "ScoreProducer is missing its run profile. Did setConf get called?")
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

    // Load model definition.
    _modelDefinition = Option(conf.get(ScoringProducer.modelDefinitionConfKey))
        .map(ModelDefinition.fromJson)
    require(_modelDefinition.isDefined, "A ModelDefinition was not specified!")

    // Load run profile.
    _modelEnvironment = Option(conf.get(ScoringProducer.modelEnvironmentConfKey))
        .map(ModelEnvironment.fromJson)
    require(_modelEnvironment.isDefined, "A ModelEnvironment was not specified!")
  }
}
