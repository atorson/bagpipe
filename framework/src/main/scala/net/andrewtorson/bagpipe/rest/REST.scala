/*
 *
 * Author: Andrew Torson
 * Date: Dec 12, 2016
 */

package net.andrewtorson.bagpipe.rest



import java.sql.Date

import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.{Directives, RequestContext, Route}
import net.andrewtorson.bagpipe.utils._
import scala.util.{Failure, Success, Try}

import akka.NotUsed
import akka.http.scaladsl.marshalling.{PredefinedToResponseMarshallers, ToResponseMarshallable}
import net.andrewtorson.bagpipe.{ApplicationService, ApplicationServiceUnavailableException}
import net.andrewtorson.bagpipe.eventbus.{EntityBusEvent, EventBusClassifiers, EventBusTopicID}
import net.andrewtorson.bagpipe.eventbus.EventBusClassifiers._
import de.heikoseeberger.akkasse.{EventStreamMarshalling, ServerSentEvent}




/**
 * Created by Andrew Torson on 12/12/16.
 */
trait REST[E<: BaseEntity[E]] extends Directives {

  import scala.concurrent.duration._
  import akka.http.scaladsl.marshalling.PredefinedToResponseMarshallers._

  @throws(classOf[ApplicationServiceUnavailableException])
  protected def serviceBus = ApplicationService[ServiceBusModule]

  protected val entityDefinition: EntityDefinition[E]

  import de.heikoseeberger.akkasse.EventStreamMarshalling._

  lazy val path = entityDefinition.categoryID.asInstanceOf[NameKeyID].nameKey
  implicit lazy val toJsonTransformer = ProtoJsonSupport.toJsonStringTransformer[E]
  implicit lazy val tag = entityDefinition.tag
  implicit lazy val entityJsonMarshaller = ProtoJsonSupport.jsonProtoMarshaller[E]
  implicit lazy val entityJsonUnmarshaller = ProtoJsonSupport.jsonProtoUnmarshaller[E](entityDefinition.companion)
  implicit lazy val entityCollectionJsonMarshaller = ProtoJsonSupport.jsonProtoArrayMarshaller[E]

  protected def GET = (get & path(path / "ByKey" / RemainingPath)) { id => {
    val key = KeyOneOf[E](id.toString())(entityDefinition.tag)
    extractRequestContext{ ctx: RequestContext =>
      onComplete{
        import ctx.materializer
        ctx.request.discardEntityBytes().future
      }{case _ => onComplete {
          val event = EntityBusEvent(key, EventBusClassifiers(REST_READ, trivialPredicate()))
          val f = serviceBus.receiveOne(topic(RETRIEVED, MISSING, FAILED)(entityDefinition.categoryID): _*)(5.seconds)(Some(event.correlationID))
          serviceBus.deliver(event)
          f
        }{
          case Success(opt) => opt match {
            case Some(classified: EntityBusEvent) => (classified.value, classified.topic) match {
              case (EntityOneOf(ent), _) if (ent.getClass == key.etag.runtimeClass) => complete(ent.asInstanceOf[E])
              case (_, x: EventBusTopicID) if (x.isMessageSub(MISSING)) => complete(NotFound, s"Entity $id doesn't exist")
              case _ => complete(InternalServerError, "Internal data store transaction failure")
            }
            case _ => complete(InternalServerError, "Internal data store transaction failure")
          }
          case Failure(ex) => complete(InternalServerError, s"An error occurred: ${ex.getMessage}")
      }}
   }}}

  protected def DELETE = (delete & path(path / RemainingPath)) { id => {
    val key = KeyOneOf[E](id.toString())(entityDefinition.tag)
    extractRequestContext{ ctx: RequestContext =>
      onComplete{
        import ctx.materializer
        ctx.request.discardEntityBytes().future
      }{case _ => onComplete {
          val event = EntityBusEvent(key, EventBusClassifiers(REST_DELETE, trivialPredicate()))
          val f = serviceBus.receiveOne(topic(DELETED, FAILED)(entityDefinition.categoryID): _*)(5.seconds)(Some(event.correlationID))
          serviceBus.deliver(event)
          f
        }{
          case Success(opt) => opt match {
            case Some(classified: EntityBusEvent) => classified.topic match {
              case x: EventBusTopicID if (x.isMessageSub(DELETED)) => complete(OK, s"Entity ${key} has been successfully deleted")
              case _ => complete(BadRequest, s"Entity $key either doesn't exist or can't be deleted")
            }
            case _ => complete(InternalServerError, "Internal data store transaction failure")
          }
          case Failure(ex) => complete(InternalServerError, s"An error occurred: ${ex.getMessage}")
       }}
    }}}

  protected def POST = (post & path(path)){
    entity(as[E]) { entityToInsert =>
      onComplete {
        val event = EntityBusEvent(EntityOneOf(entityToInsert.enforceAuditIfApplicable(false)), EventBusClassifiers(REST_CREATE, entityPredicate()))
        val f = serviceBus.receiveOne(topic(CREATED, FAILED)(entityDefinition.categoryID): _*)(5.seconds)(Some(event.correlationID))
        serviceBus.deliver(event)
        f
      } {
        case Success(opt) => opt match {
          case Some(classified: EntityBusEvent) =>
            classified.value match {
              case EntityOneOf(ent) if (ent.getClass == entityToInsert.getClass) => complete(Created, s"Entity ${entityToInsert.namekey} has been successfully created")
              case _ => complete(BadRequest, s"Entity ${entityToInsert.namekey} already exists")
            }
          case _ => complete(InternalServerError, "Internal data store transaction failure")
        }
        case Failure(ex) => complete(InternalServerError, s"An error occurred: ${ex.getMessage}")
      }
   }}

  protected def PUT = (put & path(path)) {
    entity(as[E]) { entityToInsert =>
      onComplete {
        val event = EntityBusEvent(EntityOneOf(entityToInsert.enforceAuditIfApplicable(false)), EventBusClassifiers(REST_UPDATE, entityPredicate()))
        val f = serviceBus.receiveOne(topic(UPDATED, FAILED)(entityDefinition.categoryID): _*)(5.seconds)(Some(event.correlationID))
        serviceBus.deliver(event)
        f
      } {
        case Success(opt) => opt match {
          case Some(classified: EntityBusEvent) =>
            classified.value match {
              case EntityOneOf(ent) if (ent.getClass == entityToInsert.getClass) => complete(OK, s"Entity ${entityToInsert.namekey} has been successfully updated")
              case _ => complete(BadRequest, s"Entity ${entityToInsert.namekey} does not exist")
            }
          case _ => complete(InternalServerError, "Internal data store transaction failure")
        }
        case Failure(ex) => complete(InternalServerError, s"An error occurred: ${ex.getMessage}")
      }
  }}

  protected def POLL = (get & path(path / "UpdatedSince")) {
    extractRequestContext{ ctx: RequestContext =>
      onComplete{
        import ctx.materializer
        ctx.request.discardEntityBytes().future
      }{ case _ =>
      parameterMap { fieldMap => {
        val now = System.currentTimeMillis()
        fieldMap.get("sinceTime").map(v => Try{v.toLong}) match {
          case Some(Success(sinceMillis: Long)) if (sinceMillis <= now) =>
            onComplete {
              val eventSince = EntityBusEvent[E](KeyOneOf(""), EventBusClassifiers(REST_POLL, trivialPredicate()), new Date(sinceMillis))
              val eventBy = EntityBusEvent[E](KeyOneOf(""), EventBusClassifiers(REST_POLL, trivialPredicate()), new Date(now))
              val f = serviceBus.receiveUntil(topic(RETRIEVED, CREATED, UPDATED, POLLED, FAILED)(entityDefinition.categoryID): _*)(5.seconds)(
                _.topic.isMessageSub(POLLED), Some(eventSince.correlationID))
              serviceBus.deliver(eventSince, eventBy)
              f
            } {
              case Success(interim) => Try {
                interim.map(_.value.entity.get.asInstanceOf[E])
              } match {
                case Success(result) => complete(result)
                case _ => complete(InternalServerError, "Internal data store transaction failure")
              }
              case Failure(ex) => complete(InternalServerError, s"An error occurred: ${ex.getMessage}")
            }
          case _ => complete(BadRequest, s"Could not extract a valid sinceTime parameter value from the request query params $fieldMap")
        }
      }}}
    }}

  protected def STREAM = (get & path(path / "Stream")) {
    extractRequestContext{ ctx: RequestContext =>
      onComplete {
        import ctx.materializer
        ctx.request.discardEntityBytes().future
      }{ case _ => complete {
        val s = serviceBus.source(topic(REST_CREATED, REST_UPDATED)(entityDefinition.categoryID): _*)()._2.collect[ServerSentEvent] {
          case x: EntityBusEvent if (x.value.isHydrated) => {
            val e = x.value.entity.get.asInstanceOf[E].enforceAuditIfApplicable(false)
            ServerSentEvent(Some(toJsonTransformer(e)), Some(entityDefinition.categoryID.nameKey), Some(e.namekey))
          }
        }.mapMaterializedValue(_ => NotUsed)
        ToResponseMarshallable(s)
      }}
    }
  }

  lazy val routes: Route = POST ~ PUT ~ POLL ~ GET ~ DELETE ~ STREAM
}
