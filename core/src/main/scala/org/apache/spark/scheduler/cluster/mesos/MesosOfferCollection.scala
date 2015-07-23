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

import scala.collection.JavaConversions._

import org.apache.mesos.protobuf.GeneratedMessage
import org.apache.mesos.Protos.{ Offer, OfferID, Resource, SlaveID, Value }

/**
 * @pre All supplied offers have the same SlaveID
 */
protected[spark] case class MesosOfferCollection(offers: Seq[Offer])
    extends MesosSchedulerUtils {

  import org.apache.mesos.Protos.Value.Type.SCALAR

  def getHostname(): String =
    offers.head.getHostname

  def getSlaveId(): SlaveID =
    offers.head.getSlaveId

  def getResources(name: String, resourceFilter: Resource => Boolean): Seq[Resource] =
    offers.flatMap { offer =>
      offer.getResourcesList.filter { r =>
        r.getName == name && resourceFilter(r)
      }
    }

  def scalarResourceSum(name: String, resourceFilter: Resource => Boolean): Resource = {
    val newFilter = (r: Resource) => r.getType == SCALAR && resourceFilter(r)
    val resources = getResources(name, newFilter)

    if (resources.isEmpty)
      Resource.newBuilder
        .setName(name)
        .setType(SCALAR)
        .setScalar(Value.Scalar.newBuilder.setValue(0.0))
        .build
    else
      resources.reduceLeft { (acc, r) =>
        val sum = acc.getScalar.getValue + r.getScalar.getValue
        acc.toBuilder
          .setScalar(Value.Scalar.newBuilder.setValue(sum))
          .build
      }
  }

  def mem(resourceFilter: Resource => Boolean): Double =
    scalarResourceSum("mem", resourceFilter).getScalar.getValue

  def cpus(resourceFilter: Resource => Boolean): Double =
    scalarResourceSum("cpus", resourceFilter).getScalar.getValue

  def getAttributes(): Map[OfferID, Map[String, GeneratedMessage]] =
    offers
      .map(offer => offer.getId -> toAttributeMap(offer.getAttributesList))
      .toMap
      .withDefaultValue(Map.empty[String, GeneratedMessage])

}

object MesosOfferCollection {
  private[mesos] def defaultResourceFilter: Resource => Boolean =
    (r: Resource) => true

  private[mesos] def revocableResourceFilter: Resource => Boolean =
    (r: Resource) => r.hasRevocable

}
