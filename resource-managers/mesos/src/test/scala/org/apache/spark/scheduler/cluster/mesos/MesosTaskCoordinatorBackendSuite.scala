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

import java.net.URL

import org.mockito.Mockito._
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.mockito.MockitoSugar

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.executor.CoarseGrainedExecutorBackend
import org.apache.spark.scheduler.TaskSchedulerImpl



class MesosTaskCoordinatorBackendSuite extends SparkFunSuite
    with LocalSparkContext
    with MockitoSugar
    with BeforeAndAfter
    with ScalaFutures {
  private var backend: MesosTaskCoordinatorBackend = _
  private var taskScheduler: TaskSchedulerImpl = _
  private var sparkConf: SparkConf = _

  test("mesos task coordinator starts") {
    sparkConf = (new SparkConf)
      .setMaster("local[*]")
      .setAppName("test-mesos-sdk")
      .set("spark.mesos.backend", "sdk")
    sc = new SparkContext(sparkConf)
    taskScheduler = mock[TaskSchedulerImpl]
    when(taskScheduler.sc).thenReturn(sc)
    backend = new MesosTaskCoordinatorBackend(taskScheduler, sc)
    backend.start()
    assert(!backend.isReady())
  }
}
