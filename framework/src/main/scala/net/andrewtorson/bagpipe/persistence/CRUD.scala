/*
 *
 * Author: Andrew Torson
 * Date: Oct 28, 2016
 */

package net.andrewtorson.bagpipe.persistence

import java.util.Date

import scala.concurrent.{Await, ExecutionContext, Future, Promise}

import akka.Done
import net.andrewtorson.bagpipe.utils.{OneOf, _}
import scala.collection.mutable.{Buffer, _}
import scala.reflect._
import scala.util.{Failure, Success, Try}

import _root_.io.getquill.context.async.TransactionalExecutionContext
import net.andrewtorson.bagpipe.{ApplicationService, ApplicationServiceUnavailableException}
import net.andrewtorson.bagpipe.entities.{AuditDef, Timestamp}
import net.andrewtorson.bagpipe.networking.{BE, OE}


/**
 * Created by Andrew Torson on 10/28/16.
 */

trait CRUD[E<: BaseEntity[E]] {

  @throws(classOf[ApplicationServiceUnavailableException])
  protected def context: BagpipePostgreSQLContext = ApplicationService[PersistenceModule].context

  protected val entityDefinition: EntityDefinition[E]

  protected def internalCreateOrUpdate(entity: E, enforceCreate: Boolean, enforceUpdate: Boolean)(implicit ec: ExecutionContext): Future[Either[Done,Done]]

  protected def internalDelete(namekey: String)(implicit ec: ExecutionContext): Future[Done]

  protected def internalRead(namekey: String)(implicit ec: ExecutionContext): Future[Option[E]]

  protected def internalPoll(since: Timestamp, by: Timestamp)(implicit ec: ExecutionContext): Future[List[E]]

  def poll(since: Date = new Date(0), by: Date = new Date())(implicit ec: ExecutionContext): Future[List[E]] = Try{context} match {
    case Success(context) => {
      ec match {
        case TransactionalExecutionContext(_,_) => internalPoll(Timestamp(millis = since.getTime), Timestamp(millis = by.getTime))(ec)
        case _ =>  context.transaction{ tc: TransactionalExecutionContext =>
          internalPoll(Timestamp(millis = since.getTime), Timestamp(millis = by.getTime))(tc)
        }
      }}
    case Failure(exc) => Future.failed(exc)
  }


  def read(namekey: String)(implicit ec: ExecutionContext) : Future[Option[E]] =
    Try{context} match {
       case Success(context) => {
         ec match {
           case TransactionalExecutionContext(_,_) => internalRead(namekey)(ec)
           case _ =>  context.transaction{ tc: TransactionalExecutionContext =>
             internalRead(namekey)(tc)
           }
       }}
       case Failure(exc) => Future.failed(exc)
  }

  def createOrUpdate(entity: E, done: Buffer[OE]  = Buffer.empty[OE], enforceCreate: Boolean = false, enforceUpdate: Boolean = false)(implicit ec: ExecutionContext): Future[Either[Done,Done]] =
    Try{context} match {
      case Success(context) => {
        ec match {
          case TransactionalExecutionContext(_,_) => innerCreateOrUpdate(entity.enforceAuditIfApplicable(false), done, enforceCreate, enforceUpdate)(ec)
          case _ =>  context.transaction{ tc: TransactionalExecutionContext =>
            innerCreateOrUpdate(entity.enforceAuditIfApplicable(false), done, enforceCreate, enforceUpdate)(tc)
          }
      }}
      case Failure(exc) => Future.failed(exc)
   }

  def delete(namekey: String, done: Buffer[OE] = Buffer.empty[OE])(implicit ec: ExecutionContext): Future[Done] =
    Try{context} match {
      case Success(context) => {
        ec match {
          case TransactionalExecutionContext(_,_) => innerDelete(namekey, done)(ec)
          case _ =>  context.transaction{ tc: TransactionalExecutionContext =>
            innerDelete(namekey, done)(tc)
          }
      }}
      case Failure(exc) => Future.failed(exc)
    }

  private def innerDelete(namekey: String, done: Buffer[OE])(implicit ec: ExecutionContext): Future[Done] = {
    val doneKeys: Buffer[String] = done.map(_.key)
    //val doneTags: Buffer[ClassTag[_]] = done.map(_.etag)
    if (doneKeys.contains(namekey)) Promise[Done].success(Done).future else {
      //ToDo: refactor to query all entity definitions and delete all entities pointing to this one.
      val cascadeFields = for (field <- entityDefinition.nested
                               if ((field.subentityDefinition.tag == AuditDef.tag || field.subentityDefinition.nested.filter(_.subentityDefinition == entityDefinition).size == 1)
                                 /*&& !doneTags.contains(field.subentityDefinition.tag)*/)) yield field
      val cascade = if (!cascadeFields.isEmpty) {
        val v = Await.result(read(namekey), scala.concurrent.duration.Duration.Inf).get
        for (field <- cascadeFields) yield (field, field.getScalar(v))
      } else Nil
      var result: Future[Done] = internalDelete(namekey)(ec)
      done += KeyOneOf[E](namekey)(entityDefinition.tag)
      for ((field, key) <- cascade if (!doneKeys.contains(key))) {
        result = result.flatMap{_ => field.subentityDefinition.crud.delete(key, done)(ec)}
      }
      result
    }
  }


  def innerCreateOrUpdate(entity: E, done: Buffer[OE], enforceCreate: Boolean, enforceUpdate: Boolean)(implicit ec: ExecutionContext): Future[Either[Done,Done]] = {
    val doneKeys: Buffer[String] = done.map(_.key)
    require(!entity.namekey.isEmpty, "Entity must have a namekey defined")
    if (doneKeys.contains(entity.namekey)){
      // second call always returns update
      Promise[Either[Done,Done]].success(Left(Done)).future
    } else {
      for (field <- entity.definition.nested) {
        require(field.getScalar(entity).isInstanceOf[String], "Entity must have all nested field namekeys defined")
      }
      var result: Future[Either[Done, Done]] = if (enforceCreate == enforceUpdate) {
        entity.enforceAuditIfApplicable(false).version match{
          case z if (z > 1) =>   internalCreateOrUpdate(entity, enforceCreate = false, enforceUpdate = true)(ec)
          case _ =>  internalCreateOrUpdate(entity, enforceCreate = true, enforceUpdate = false)(ec)
        }
      } else {
        internalCreateOrUpdate(entity, enforceCreate, enforceUpdate)(ec)
      }
      done += EntityOneOf[E](entity)
      for (field <- entity.definition.nested) {
        implicit val ctag = field.subentityDefinition.tag
        entity.get(field) match {
          case EntityOneOf(e: field.Sub) if !doneKeys.contains(e.namekey) => {
            result = result.flatMap {x: Either[Done, Done] => e.enforceAuditIfApplicable(false).version match {
                case z if z > 1 => field.subentityDefinition.crud.createOrUpdate(e, done, enforceCreate = false, enforceUpdate = true)(ec).map(_ => x)
                case _ => field.subentityDefinition.crud.createOrUpdate(e, done, enforceCreate = true, enforceUpdate = false)(ec).map(_ => x)
            }}
          }
          case _ => {}
        }
      }
      result
    }
  }

}



