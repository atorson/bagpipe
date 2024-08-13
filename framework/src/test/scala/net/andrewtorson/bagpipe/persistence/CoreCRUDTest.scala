/*
 *
 * Author: Andrew Torson
 * Date: Oct 28, 2016
 */

package net.andrewtorson.bagpipe.persistence

import java.util.Date

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.{global => ec}
import scala.concurrent.duration._
import akka.util.Timeout
import net.andrewtorson.bagpipe.ApplicationService
import net.andrewtorson.bagpipe.entities._
import net.andrewtorson.bagpipe.utils.{CoreTestData, PersistenceModule}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import scala.util.{Success, Try}



class CoreCRUDTest extends FunSuite with AbstractPersistenceTest with BeforeAndAfterEach{
  implicit val timeout = Timeout(5.seconds)
  val modules = new Modules {}
  val data = CoreTestData
  import Statistic.StatisticStatus._

  ApplicationService[PersistenceModule].start

  test("Testing Core CRUD") {
    val start = new Date(new Date().getTime)
    Await.result(StatisticDef.crud.createOrUpdate(data.statistic),5.seconds)
    assert (Await.result(StatisticDef.crud.poll(start),5.seconds).size == 1, "Test entities were not polled")
    assert (Await.result(StatisticDef.crud.poll(new Date()),5.seconds).isEmpty, "Test entities polling did not work")
    val statisticOpt = Await.result((StatisticDef.crud.read(data.statistic.namekey)),5.seconds)
    assert (!statisticOpt.isEmpty, "Test entity was not created")
    val auditOpt = statisticOpt.get.hydrateFieldValue[Audit]._1
    assert(!auditOpt.isEmpty && auditOpt.get.ver == 1, "Audit info was not properly read for a test entity")
    Await.result(StatisticDef.crud.createOrUpdate(statisticOpt.get.set(StatisticDef.Status)(Ongoing).bumpUpAuditIfApplicable),5.seconds)
    val statisticOpt2 = Await.result((StatisticDef.crud.read(data.statistic.namekey)),5.seconds)
    assert (!statisticOpt2.isEmpty && (statisticOpt2.get.get(StatisticDef.Status) != data.statistic.get(StatisticDef.Status)), "Update of the test data entity status did not work")
    val auditOpt2 = statisticOpt2.get.hydrateFieldValue[Audit]._1
    assert(!auditOpt2.isEmpty && auditOpt2.get.ver == 2, "Audit info was not properly updated for a test entity")
    Await.result(StatisticDef.crud.delete(data.statistic.namekey),5.seconds)
    assert (Await.result(StatisticDef.crud.poll(new Date(0)),5.seconds).isEmpty, "Test entities were not deleted")
    assert (Await.result(AuditDef.crud.poll(new Date(0)),5.seconds).isEmpty, "Test entities delete cascade did not work")

  }

  override protected def  afterEach(): Unit = {
    Await.result(StatisticDef.crud.read(data.statistic.namekey),5.seconds) match {
      case Some(e: Statistic) =>  {
        Await.result(Future.sequence(Seq(
          StatisticDef.crud.delete(e.namekey)
        )),5.seconds)
      }
      case _ => {}
    }
    ApplicationService[PersistenceModule].stop
    super.afterEach()
  }

}