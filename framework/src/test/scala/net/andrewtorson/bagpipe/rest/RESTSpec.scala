/*
 *
 * Author: Andrew Torson
 * Date: Aug 8, 2016
 */

package net.andrewtorson.bagpipe.rest



import java.util.Date

import scala.annotation.unchecked.uncheckedVariance
import scala.collection.mutable
import akka.http.scaladsl.model.StatusCodes._

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future, Promise}
import scala.reflect.ClassTag
import akka.{Done, NotUsed}
import akka.http.scaladsl.marshalling._
import akka.http.scaladsl.unmarshalling._
import akka.stream.{ActorAttributes, Supervision}
import akka.stream.scaladsl.Flow
import net.andrewtorson.bagpipe.ApplicationService
import net.andrewtorson.bagpipe.eventbus.EventBusClassifiers._
import net.andrewtorson.bagpipe.eventbus.{EntityBusEvent, EventBusClassifiers}
import net.andrewtorson.bagpipe.utils.{BaseEntity, _}
import net.andrewtorson.bagpipe.entities._
import net.andrewtorson.bagpipe.networking.{ED, OE}
import com.typesafe.config.Config
import org.mockito.Matchers
import org.mockito.internal.matchers.{GreaterOrEqual, LessOrEqual, LessThan}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import scala.util.{Success, Try}

class RESTSpec extends AbstractRestTest with BeforeAndAfterAll with BeforeAndAfterEach {

  val testData: TestData = CoreTestData

  class TestStreamingModuleImpl(override protected val dependency: ServiceBusModule with ActorModule) extends CoreStreamingModule {

    override protected def bootstrapFlows() = {
      EntityDefinition.all().foldLeft[Boolean](true)((b, d) => b && addCoreService(d.tag))
    }

    override protected lazy val createFlow = StreamingModule.getAsyncFlow{e: EntityBusEvent => {
      implicit val ct = e.value.etag
      e.value match {
        case (EntityOneOf(value)) => getMock[e.E].createOrUpdate(value, enforceCreate = true)(executor).collect{
          case Right(Done) => Seq(EntityBusEvent(e.value, EventBusClassifiers(DB_CREATED, entityPredicate()), correlationID = e.correlationID))
        }(executor).recover{
          case _ => Seq(EntityBusEvent(KeyOneOf[e.E](e.value.key), EventBusClassifiers(DB_FAILURE, trivialPredicate()), correlationID = e.correlationID))
        }(executor)
        case x => Future.failed[Seq[EntityBusEvent]](new IllegalArgumentException(s"Entity create request $x is invalid"))
      }}}()

    override protected lazy val updateFlow = StreamingModule.getAsyncFlow{e: EntityBusEvent => {
      implicit val ct = e.value.etag
      e.value match {
        case (EntityOneOf(value)) => getMock[e.E].createOrUpdate(value, enforceUpdate = true)(executor).collect{
          case Left(Done) => Seq(EntityBusEvent(e.value, EventBusClassifiers(DB_UPDATED, entityPredicate()), correlationID = e.correlationID))
        }(executor).recover{
          case _ => Seq(EntityBusEvent(KeyOneOf[e.E](e.value.key), EventBusClassifiers(DB_FAILURE, trivialPredicate()), correlationID = e.correlationID))
        }(executor)
        case x => Future.failed[Seq[EntityBusEvent]](new IllegalArgumentException(s"Entity update request $x is invalid"))
      }}}()

    override protected lazy val readFlow = StreamingModule.getAsyncFlow{e: EntityBusEvent => {
      implicit val ct = e.value.etag
      getMock[e.E].read(e.value.key)(executor).collect{
        case Some(x: e.E)  => Seq(EntityBusEvent(EntityOneOf(x), EventBusClassifiers(DB_RETRIEVED, entityPredicate()), correlationID = e.correlationID))
        case None => Seq(EntityBusEvent(KeyOneOf[e.E](e.value.key), EventBusClassifiers(DB_MISSING, trivialPredicate()), correlationID = e.correlationID))
      }(executor).recover{
        case _ => Seq(EntityBusEvent(KeyOneOf[e.E](e.value.key), EventBusClassifiers(DB_FAILURE, trivialPredicate()), correlationID = e.correlationID))
      }(executor)
    }}()

    override protected lazy val deleteFlow = StreamingModule.getAsyncFlow{e: EntityBusEvent => {
      implicit val ct = e.value.etag
      getMock[e.E].delete(e.value.key)(executor).collect{
        case Done => Seq(EntityBusEvent(e.value, EventBusClassifiers(DB_DELETED, trivialPredicate()), correlationID = e.correlationID))
      }(executor).recover{
        case _ => Seq(EntityBusEvent(KeyOneOf[e.E](e.value.key), EventBusClassifiers(DB_FAILURE, trivialPredicate()), correlationID = e.correlationID))
      }(executor)
    }}()



    override protected lazy val pollFlow = {
      Flow[EntityBusEvent].groupBy(EntityDefinition.all.size, _.topic).sliding(2).mapAsync(1) { s: Seq[EntityBusEvent] => {
        val e = s(0)
        implicit val ct = e.value.etag
        getMock[e.E].poll(s(0).timestamp, s(1).timestamp)(executor).map(Seq[EntityBusEvent]() ++ _.map(x => {
          EntityBusEvent[e.E](EntityOneOf(x), EventBusClassifiers(DB_RETRIEVED, entityPredicate()), correlationID = e.correlationID)
        }) :+ EntityBusEvent[e.E](e.value, EventBusClassifiers(DB_POLLED, trivialPredicate()), correlationID = e.correlationID))(executor).recover {
          case _ => Seq(EntityBusEvent(KeyOneOf[e.E](e.value.key), EventBusClassifiers(DB_FAILURE, trivialPredicate()), correlationID = e.correlationID))
        }(executor)
      }
      }.withAttributes(ActorAttributes.supervisionStrategy(Supervision.resumingDecider)).mergeSubstreams

    }

    ApplicationService.register[StreamingModule](this)
  }

  val modules = new TestConfigurationModule with TestActorModuleImpl
  implicit val serviceBusModule = new ServiceBusModuleImpl(modules)
  val testModule = new TestStreamingModuleImpl(serviceBusModule)

  override def testConfig: Config = new TestConfigurationModule {}.config


  "Routes" should {

    "Get: return Not Found for empty resources" in {
      runTest(new RestfulTest {
        override def innerRunTest[E <: BaseEntity[E]](rest: REST[E])(implicit ct: ClassTag[E], marshaller: ToEntityMarshaller[E], unmarshaller: FromEntityUnmarshaller[E],  arrayMarshaller: ToEntityMarshaller[List[E]], arrayUnmarshaller: FromEntityUnmarshaller[List[E]]) = {
          val namekey = testData.getTestEntityName[E]
          getMock[E].read(namekey) returns Promise.successful(None).future
          Get(s"/${rest.path}/ByKey/$namekey") ~> rest.routes ~> check {
            handled shouldEqual true
            status shouldEqual NotFound
          }
        }
      })
    }

    "Get: return an array with 1 entity for a given resource" in {
      runTest(new RestfulTest {
        override def innerRunTest[E <: BaseEntity[E]](rest: REST[E])(implicit ct: ClassTag[E], marshaller: ToEntityMarshaller[E], unmarshaller: FromEntityUnmarshaller[E],  arrayMarshaller: ToEntityMarshaller[List[E]], arrayUnmarshaller: FromEntityUnmarshaller[List[E]]) = {
          val namekey = testData.getTestEntityName[E]
          getMock[E].read(namekey) returns Promise.successful(Some(testData.getTestEntity[E])).future
          Get(s"/${rest.path}/ByKey/$namekey") ~> rest.routes ~> check {
            handled shouldEqual true
            status shouldEqual OK
            responseAs[Option[E]].isEmpty shouldEqual false
          }
        }
      })
    }

    "Delete: complain about non-existing resources" in {
      runTest(new RestfulTest {
        override def innerRunTest[E <: BaseEntity[E]](rest: REST[E])(implicit ct: ClassTag[E], marshaller: ToEntityMarshaller[E], unmarshaller: FromEntityUnmarshaller[E],  arrayMarshaller: ToEntityMarshaller[List[E]], arrayUnmarshaller: FromEntityUnmarshaller[List[E]]) = {
          val namekey = testData.getTestEntityName[E]
          getMock[E].delete(namekey) returns Promise.failed(new IllegalStateException("Entity does not exists")).future
          Delete(s"/${rest.path}/$namekey") ~> rest.routes ~> check {
            handled shouldEqual true
            status shouldEqual BadRequest
          }
        }
      })
    }

    "Delete: successfully process a valid delete request for a given resource" in {
      runTest(new RestfulTest {
        override def innerRunTest[E <: BaseEntity[E]](rest: REST[E])(implicit ct: ClassTag[E], marshaller: ToEntityMarshaller[E], unmarshaller: FromEntityUnmarshaller[E],  arrayMarshaller: ToEntityMarshaller[List[E]], arrayUnmarshaller: FromEntityUnmarshaller[List[E]]) = {
          val namekey = testData.getTestEntityName[E]
          val entityOption = Some(testData.getTestEntity[E])
          getMock[E].delete(namekey) returns Promise.successful(Done).future
          Delete(s"/${rest.path}/$namekey") ~> rest.routes ~> check {
            handled shouldEqual true
            status shouldEqual OK
          }
        }
      })
    }

    "Post: create entity with the json in post" in {
      runTest(new RestfulTest {
        override def innerRunTest[E <: BaseEntity[E]](rest: REST[E])(implicit ct: ClassTag[E], marshaller: ToEntityMarshaller[E], unmarshaller: FromEntityUnmarshaller[E],  arrayMarshaller: ToEntityMarshaller[List[E]], arrayUnmarshaller: FromEntityUnmarshaller[List[E]]) = {
          val namekey = testData.getTestEntityName[E]
          val entity = testData.getTestEntity[E]
          getMock[E].createOrUpdate(any[E], any[mutable.Buffer[OE]], any[Boolean], any[Boolean])(any[ExecutionContext]) returns Promise.successful(Right(Done)).future
          Post(s"/${rest.path}", entity) ~> rest.routes ~> check {
            handled shouldEqual true
            status shouldEqual Created
          }
        }
      })
    }

    "Post: complain about already existing entities" in {
      runTest(new RestfulTest {
        override def innerRunTest[E <: BaseEntity[E]](rest: REST[E])(implicit ct: ClassTag[E], marshaller: ToEntityMarshaller[E], unmarshaller: FromEntityUnmarshaller[E],  arrayMarshaller: ToEntityMarshaller[List[E]], arrayUnmarshaller: FromEntityUnmarshaller[List[E]]) = {
          val namekey = testData.getTestEntityName[E]
          val entity = testData.getTestEntity[E]
          getMock[E].createOrUpdate(any[E], any[mutable.Buffer[OE]], any[Boolean], any[Boolean])(any[ExecutionContext]) returns Promise.failed(new IllegalStateException("Entity exists")).future
          Post(s"/${rest.path}", entity) ~> rest.routes ~> check {
            handled shouldEqual true
            status shouldEqual BadRequest
          }
        }
      })
    }

    "Post: not handle the invalid json" in {
      runTest(new RestfulTest {
        override def innerRunTest[E <: BaseEntity[E]](rest: REST[E])(implicit ct: ClassTag[E], marshaller: ToEntityMarshaller[E], unmarshaller: FromEntityUnmarshaller[E],  arrayMarshaller: ToEntityMarshaller[List[E]], arrayUnmarshaller: FromEntityUnmarshaller[List[E]]) = {
          Post(s"/${rest.path}", "{\"name\":\"1\"}") ~> rest.routes ~> check {
            handled shouldEqual false
          }
        }
      })
    }

    "Post: not handle an empty post" in {
      runTest(new RestfulTest {
        override def innerRunTest[E <: BaseEntity[E]](rest: REST[E])(implicit ct: ClassTag[E], marshaller: ToEntityMarshaller[E], unmarshaller: FromEntityUnmarshaller[E],  arrayMarshaller: ToEntityMarshaller[List[E]], arrayUnmarshaller: FromEntityUnmarshaller[List[E]]) = {
          Post(s"/${rest.path}") ~> rest.routes ~> check {
            handled shouldEqual false
          }
        }
      })
    }

    "Put: update entity with the json in post" in {
      runTest(new RestfulTest {
        override def innerRunTest[E <: BaseEntity[E]](rest: REST[E])(implicit ct: ClassTag[E], marshaller: ToEntityMarshaller[E], unmarshaller: FromEntityUnmarshaller[E],  arrayMarshaller: ToEntityMarshaller[List[E]], arrayUnmarshaller: FromEntityUnmarshaller[List[E]]) = {
          val namekey = testData.getTestEntityName[E]
          val entity = testData.getTestEntity[E]
          getMock[E].createOrUpdate(any[E], any[mutable.Buffer[OE]], any[Boolean], any[Boolean])(any[ExecutionContext]) returns Promise.successful(Left(Done)).future
          Put(s"/${rest.path}", entity) ~> rest.routes ~> check {
            handled shouldEqual true
            status shouldEqual OK
          }
        }
      })
    }

    "Put: complain about non-existing entities" in {
      runTest(new RestfulTest {
        override def innerRunTest[E <: BaseEntity[E]](rest: REST[E])(implicit ct: ClassTag[E], marshaller: ToEntityMarshaller[E], unmarshaller: FromEntityUnmarshaller[E],  arrayMarshaller: ToEntityMarshaller[List[E]], arrayUnmarshaller: FromEntityUnmarshaller[List[E]]) = {
          val namekey = testData.getTestEntityName[E]
          val entity = testData.getTestEntity[E]
          getMock[E].createOrUpdate(any[E], any[mutable.Buffer[OE]], any[Boolean], any[Boolean])(any[ExecutionContext]) returns Promise.failed(new IllegalStateException("Entity does not exist")).future
          Put(s"/${rest.path}", entity) ~> rest.routes ~> check {
            handled shouldEqual true
            status shouldEqual BadRequest
          }
        }
      })
    }

    "Put: not handle the invalid json" in {
      runTest(new RestfulTest {
        override def innerRunTest[E <: BaseEntity[E]](rest: REST[E])(implicit ct: ClassTag[E], marshaller: ToEntityMarshaller[E], unmarshaller: FromEntityUnmarshaller[E],  arrayMarshaller: ToEntityMarshaller[List[E]], arrayUnmarshaller: FromEntityUnmarshaller[List[E]]) = {
          Put(s"/${rest.path}", "{\"name\":\"1\"}") ~> rest.routes ~> check {
            handled shouldEqual false
          }
        }
      })
    }

    "Put: not handle an empty post" in {
      runTest(new RestfulTest {
        override def innerRunTest[E <: BaseEntity[E]](rest: REST[E])(implicit ct: ClassTag[E], marshaller: ToEntityMarshaller[E], unmarshaller: FromEntityUnmarshaller[E],  arrayMarshaller: ToEntityMarshaller[List[E]], arrayUnmarshaller: FromEntityUnmarshaller[List[E]]) = {
          Put(s"/${rest.path}") ~> rest.routes ~> check {
            handled shouldEqual false
          }
        }
      })
    }

    "Poll: fetch a list of entities" in {
      runTest(new RestfulTest {
        override def innerRunTest[E <: BaseEntity[E]](rest: REST[E])(implicit ct: ClassTag[E], marshaller: ToEntityMarshaller[E], unmarshaller: FromEntityUnmarshaller[E],  arrayMarshaller: ToEntityMarshaller[List[E]], arrayUnmarshaller: FromEntityUnmarshaller[List[E]]) = {
          val currentTime: Date = new Date()
          getMock[E].poll(Matchers.argThat(new GreaterOrEqual(currentTime)), any[Date])(any[ExecutionContext]) returns Promise.successful(List[E]()).future
          getMock[E].poll(Matchers.argThat(new LessThan(currentTime)), any[Date])(any[ExecutionContext]) returns Promise.successful(List[E](testData.getTestEntity[E])).future
          Get(s"/${rest.path}/UpdatedSince?sinceTime=0") ~> rest.routes ~> check {
            handled shouldEqual true
            status shouldEqual OK
            responseAs[List[E]].isEmpty shouldEqual false
          }
        }
      })
    }

    "Poll: fetch an empty list of entities" in {
      runTest(new RestfulTest {
        override def innerRunTest[E <: BaseEntity[E]](rest: REST[E])(implicit ct: ClassTag[E], marshaller: ToEntityMarshaller[E], unmarshaller: FromEntityUnmarshaller[E],  arrayMarshaller: ToEntityMarshaller[List[E]], arrayUnmarshaller: FromEntityUnmarshaller[List[E]]) = {
          val currentTime: Date = new Date()
          getMock[E].poll(Matchers.argThat(new GreaterOrEqual(currentTime)), any[Date])(any[ExecutionContext]) returns Promise.successful(List[E]()).future
          getMock[E].poll(Matchers.argThat(new LessThan(currentTime)), any[Date])(any[ExecutionContext]) returns Promise.successful(List[E](testData.getTestEntity[E])).future
          Get(s"/${rest.path}/UpdatedSince?sinceTime=${currentTime.getTime}") ~> rest.routes ~> check {
            handled shouldEqual true
            status shouldEqual OK
            responseAs[List[E]].isEmpty shouldEqual true
          }
        }
      })
    }

    "Poll: fetch a bad request" in {
      runTest(new RestfulTest {
        override def innerRunTest[E <: BaseEntity[E]](rest: REST[E])(implicit ct: ClassTag[E], marshaller: ToEntityMarshaller[E], unmarshaller: FromEntityUnmarshaller[E],  arrayMarshaller: ToEntityMarshaller[List[E]], arrayUnmarshaller: FromEntityUnmarshaller[List[E]]) = {
          val currentTime: Date = new Date()
          getMock[E].poll(Matchers.argThat(new GreaterOrEqual(currentTime)), any[Date])(any[ExecutionContext]) returns Promise.successful(List[E]()).future
          getMock[E].poll(Matchers.argThat(new LessThan(currentTime)), any[Date])(any[ExecutionContext]) returns Promise.successful(List[E](testData.getTestEntity[E])).future
          Get(s"/${rest.path}/UpdatedSince?sinceTime=${2*currentTime.getTime}") ~> rest.routes ~> check {
            handled shouldEqual true
            status shouldEqual BadRequest
          }
        }
      })
    }

    "Poll: complain about invalid time parameter" in {
      runTest(new RestfulTest {
        override def innerRunTest[E <: BaseEntity[E]](rest: REST[E])(implicit ct: ClassTag[E], marshaller: ToEntityMarshaller[E], unmarshaller: FromEntityUnmarshaller[E],  arrayMarshaller: ToEntityMarshaller[List[E]], arrayUnmarshaller: FromEntityUnmarshaller[List[E]]) = {
          Get(s"/${rest.path}/UpdatedSince?blah=blah") ~> rest.routes ~> check {
            handled shouldEqual true
            status shouldEqual BadRequest
          }
        }
      })
    }

  }

  override protected def beforeAll() = {
    super.beforeAll()
    testData.testEntityDefinitions.foreach(x => initEntity(x.tag))
    testModule.start
    serviceBusModule.start
  }

  private def initEntity[E <: BaseEntity[E]] (implicit ct: ClassTag[E]) = {
    setMock[E]
  }

  private def runTest(test: RestfulTest): Unit = {
    testData.testEntityDefinitions.filter(x => Try{testData.getTestEntity(x.tag)}.isSuccess).foreach(x => test.runTest(x.tag))
  }


  trait RestfulTest {

    def runTest[E <: BaseEntity[E]](implicit ct: ClassTag[E]): Unit = {
      implicit val entityJsonMarshaller = ProtoJsonSupport.jsonProtoMarshaller[E]
      implicit val entityJsonUnmarhsaller = ProtoJsonSupport.jsonProtoUnmarshaller[E](EntityDefinition[E].companion)
      implicit val entityArrayJsonMarshaller = ProtoJsonSupport.jsonProtoArrayMarshaller[E]
      implicit val entityArrayJsonUnmarshaller = ProtoJsonSupport.jsonProtoArrayUnmarshaller[E](EntityDefinition[E].companion)
      innerRunTest[E](EntityDefinition[E].rest)
    }

    def innerRunTest[E <: BaseEntity[E]](rest: REST[E])(implicit ct: ClassTag[E], marshaller: ToEntityMarshaller[E], unmarshaller: FromEntityUnmarshaller[E],
      arrayMarshaller: ToEntityMarshaller[List[E]], arrayUnmarshaller: FromEntityUnmarshaller[List[E]]): Unit

  }




}