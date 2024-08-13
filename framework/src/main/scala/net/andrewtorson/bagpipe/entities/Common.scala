/*
 *
 * Author: Andrew Torson
 * Date: Dec 13, 2016
 */

package net.andrewtorson.bagpipe.entities


import java.util.Date
import javax.ws.rs.Path

import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag

import akka.{Done, NotUsed}
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.RequestContext
import akka.stream.UniqueKillSwitch
import akka.stream.scaladsl.{RunnableGraph, Sink, Source}
import net.andrewtorson.bagpipe.eventbus.EventBusEntityID
import net.andrewtorson.bagpipe.messaging.MQ
import net.andrewtorson.bagpipe.networking.IO
import net.andrewtorson.bagpipe.persistence.CRUD
import net.andrewtorson.bagpipe.rest.REST
import net.andrewtorson.bagpipe.utils.{EntityDefinition, _}
import io.scalac.amqp.{Connection, Message, Queue}
import io.swagger.annotations._





/**
 * Created by Andrew Torson on 11/2/16.
 */


/**
 * Audit entity
 */


object AuditDef extends EntityDefinition[Audit] {

  override def tag = ClassTag(classOf[Audit])
  override def companion = Audit

  override def newHollowInstance(namekey: String): Audit = {
    val now = Timestamp.defaultInstance.withMillis(new Date().getTime)
    companion.defaultInstance.withNamekey(namekey).withVer(1).withCrt(now).withUpd(now)
  }

  override def categoryID = EventBusEntityID(EventBusEntityID.EntityCategory, "Audit")


  val crt = new SimpleFieldDefinition{
    override type V = Timestamp
    override val vtag = ClassTag[Timestamp](classOf[Timestamp])
    override val descriptor =  companion.descriptor.findFieldByName("crt")
    override def set(entity: Audit, value: Timestamp): Audit = entity.withCrt(value)
  }

  val upd = new SimpleFieldDefinition{
    override type V = Timestamp
    override val vtag = ClassTag[Timestamp](classOf[Timestamp])
    override val descriptor =  companion.descriptor.findFieldByName("upd")
    override def set(entity: Audit, value: Timestamp): Audit = entity.withUpd(value)
  }

  val ver = new SimpleFieldDefinition{
    override type V = Int
    override val vtag = ClassTag[Int](classOf[Int])
    override val descriptor =  companion.descriptor.findFieldByName("ver")
    override def set(entity: Audit, value: Int): Audit = entity.withVer(value)
  }



  override def fields = Set[FieldDefinition](Namekey, crt, upd, ver)

  override def nested = Set[NestedFieldDefinition]()

  override val crud = new AuditCRUD

  override val rest = new AuditREST

  override val io = new AuditIO

  override lazy val mq = new AuditMQ

  EntityDefinition.register[Audit](this)

}

trait AuditEntity extends Defined[Audit]{

  val definition = AuditDef
}

class AuditCRUD extends CRUD[Audit] {

  override val entityDefinition = AuditDef

  override protected def internalRead(namekey: String)(implicit ec: ExecutionContext): Future[Option[Audit]]
  = try {
    lazy val ctx = context
    import ctx._
    ctx.run(query[Audit].filter(s => s.namekey == lift(namekey))).map(_.headOption)(ec)
  } catch {
    case exc: Throwable => Future.failed(exc)
  }

  override protected def internalCreateOrUpdate(entity: Audit, enforceCreate: Boolean, enforceUpdate: Boolean)(implicit ec: ExecutionContext): Future[Either[Done, Done]]
  = try {

    lazy val ctx = context
    import ctx._
    (enforceCreate, enforceUpdate) match {
      case (true, false) => ctx.run(query[Audit].insert(_.namekey -> lift(entity.namekey))).collect[Either[Done, Done]] { case 1L => Right(Done) }(ec)
      case (false, true) => ctx.run(query[Audit].filter(s => s.namekey == lift(entity.namekey)).update(_.namekey -> lift(entity.namekey))).collect[Either[Done, Done]] { case 1L => Left(Done) }(ec)
      case _ => ctx.run(query[Audit].filter(s => s.namekey == lift(entity.namekey)).update(_.namekey -> lift(entity.namekey))).collect[Either[Done, Done]] { case 1L => Left(Done) }(ec)
        .recoverWith { case x: Throwable => ctx.run(query[Audit].insert(_.namekey -> lift(entity.namekey))).collect[Either[Done, Done]] { case 1L => Right(Done) }(ec) }(ec)
    }
   } catch {
    case exc: Throwable => Future.failed(exc)
   }


  override protected def internalDelete(namekey: String)(implicit ec: ExecutionContext): Future[Done]
  = try {
    lazy val ctx = context
    import ctx._
    ctx.run(query[Audit].filter(s => s.namekey == lift(namekey)).delete).collect[Done] { case 1L => Done }(ec)
  } catch {
    case exc: Throwable => Future.failed(exc)
  }

  override protected def internalPoll(since: Timestamp, by: Timestamp)(implicit ec: ExecutionContext): Future[List[Audit]]
  = try{
    lazy val ctx = context
    import ctx._
    ctx.run(query[Audit].filter(s => s.upd between(lift(since),lift(by))).sortBy(s => s.upd))
  } catch {
    case exc: Throwable => Future.failed(exc)
  }
}

@Path("/Audit")
@Api(value = "/Audit")
class AuditREST extends REST[Audit] {

  override protected val entityDefinition  = AuditDef

  @Path("/ByKey/{id}")
  @ApiOperation(value = "Return Audit", notes = "", nickname = "", httpMethod = "GET", produces = "application/json")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "id", value = "Audit Id", required = false, dataType = "string", paramType = "path")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "Return Audit", response = classOf[Audit]),
    new ApiResponse(code = 404, message = "Audit Not Found"),
    new ApiResponse(code = 500, message = "Internal server error")
  ))
  override def GET = super.GET

  override def DELETE = (delete & path(path)) {
    extractRequestContext{ ctx: RequestContext =>
      onComplete {
        import ctx.materializer
        ctx.request.discardEntityBytes().future
      }{ case _ =>
        complete(BadRequest, s"Audits are automatically deleted with auditable entities and can not be deleted separately")
      }
   }}


  override def POST =  (post & path(path)) {
    extractRequestContext{ ctx: RequestContext =>
      onComplete {
        import ctx.materializer
        ctx.request.discardEntityBytes().future
      }{ case _ =>
        complete(BadRequest, s"Audits are automatically created with auditable entities and can not be created separately")
      }
  }}

  override def PUT =  (put & path(path)) {
    extractRequestContext{ ctx: RequestContext =>
      onComplete {
        import ctx.materializer
        ctx.request.discardEntityBytes().future
      }{ case _ =>
        complete(BadRequest, s"Audits are automatically updated with auditable entities and can not be updated separately")
      }
  }}

  @Path("/UpdatedSince")
  @ApiOperation(value = "Return a list of Audit entities", notes = "Updated since the given time",
    consumes = "application/x-www-form-urlencoded", produces = "application/json", nickname = "", httpMethod = "GET")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "sinceTime", value = "", required = true, dataType = "long", allowableValues = "range[0, infinity]", defaultValue = "0", paramType = "query")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "Returned list of Audit entities", response = classOf[List[Audit]]),
    new ApiResponse(code = 400, message = "Bad Request"),
    new ApiResponse(code = 500, message = "Internal server error")
  ))
  override def POLL = super.POLL

  override def STREAM =   (get & path(path / "Stream")) {
    extractRequestContext{ ctx: RequestContext =>
      onComplete {
        import ctx.materializer
        ctx.request.discardEntityBytes().future
      }{ case _ => complete(BadRequest, s"Audits are not allowed to be pushed as server-side event stream")
      }
    }
  }
}

class AuditIO extends IO[Audit] {
  override protected val entityDefinition  = AuditDef
}

class AuditMQ extends MQ[Audit] {

  private def throwException: Nothing = throw new IllegalAccessException("Audit entity is prohibited from MQ access")

  override protected val entityDefinition  = AuditDef

  override lazy val outboundQueue = throwException
  override lazy val inboundQueue = throwException
  override lazy val routingKey = throwException

  override lazy val inboundSource = throwException
  override lazy val outboundSink = throwException
  override lazy val outboundSource = throwException
  override lazy val inboundSink = throwException

}


