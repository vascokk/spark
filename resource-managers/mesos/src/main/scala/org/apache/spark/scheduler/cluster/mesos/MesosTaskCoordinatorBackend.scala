/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.scheduler.cluster.mesos

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend

private[spark] class MesosTaskCoordinatorBackend(scheduler: TaskSchedulerImpl, sc: SparkContext)
  extends CoarseGrainedSchedulerBackend(scheduler, sc.env.rpcEnv) {

  private val practicalInfinity = 100000000

  private val targetCores = conf.getOption("spark.cores.max").map(_.toInt)

  override def sufficientResourcesRegistered(): Boolean = {
    uglyF(s"So far I have ${totalCoreCount.get()} cores, need " +
      s"${targetCores.getOrElse(practicalInfinity)}")
    totalCoreCount.get >= targetCores.getOrElse(practicalInfinity)
  }

  override def isReady(): Boolean = {
    sufficientResourcesRegistered()
  }

  override def start() {
    super.start()
  }
}
