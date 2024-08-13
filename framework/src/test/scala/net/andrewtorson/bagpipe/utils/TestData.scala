/*
 *
 * Author: Andrew Torson
 * Date: Jan 3, 2017
 */

package net.andrewtorson.bagpipe.utils

import java.util.Date

import net.andrewtorson.bagpipe.entities._
import net.andrewtorson.bagpipe.networking.ED

import scala.reflect.ClassTag

/**
 * Created by Andrew Torson on 1/3/17.
 */

trait TestData {

  val testEntityDefinitions: Seq[ED]

  def getTestEntity[E<:BaseEntity[E]](implicit ct: ClassTag[E]): E

  def getTestEntityName[E<:BaseEntity[E]](implicit ct: ClassTag[E]): String = getTestEntity[E].namekey
}

object CoreTestData extends TestData{

  val statistic = StatisticDef.newHollowInstance("SomeHourlyCounter:1-Jan-1970:00")
    .set(StatisticDef.Family)("SomeHourlyCounter")
    .set(StatisticDef.TimeFrom)(Timestamp.defaultInstance.withMillis(new Date(0).getTime))
    .set(StatisticDef.TimeTo)(Timestamp.defaultInstance.withMillis((new Date(0).getTime + 60*60*1000L)))
    .set(StatisticDef.CurrentValue)(0.0).set(StatisticDef.Status)(Statistic.StatisticStatus.Finalized)

  def getTestEntity[E<:BaseEntity[E]](implicit ct: ClassTag[E]): E = {
    ct.runtimeClass match {
      case c if c == classOf[Statistic] => statistic.enforceAuditIfApplicable(true).asInstanceOf[E]
      case _ => throw new IllegalArgumentException(s"Entity ${ct.runtimeClass} is not present in test data")
    }
  }

  val testEntityDefinitions: Seq[ED] = Seq(AuditDef, StatisticDef).map(_.asInstanceOf[ED])

}
