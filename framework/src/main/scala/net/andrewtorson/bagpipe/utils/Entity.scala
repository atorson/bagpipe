/*
 *
 * Author: Andrew Torson
 * Date: Oct 28, 2016
 */

package net.andrewtorson.bagpipe.utils



import java.util.Date

import net.andrewtorson.bagpipe.entities.{Audit, AuditDef, Timestamp}
import scala.concurrent.{Await, ExecutionContext}
import scala.reflect.ClassTag

import com.google.protobuf.Descriptors
import net.andrewtorson.bagpipe.persistence.CRUD
import com.trueaccord.scalapb.{Message, _}
import scala.concurrent.duration._
import scala.util.{Success, Try}

import com.google.protobuf.Descriptors.FieldDescriptor
import net.andrewtorson.bagpipe.eventbus.EventBusEntityID
import net.andrewtorson.bagpipe.messaging.MQ
import net.andrewtorson.bagpipe.networking.{BE, IO}
import net.andrewtorson.bagpipe.rest.REST





/**
 * Created by Andrew Torson on 10/27/16.
 */
trait BaseEntity[E<:BaseEntity[E]] extends GeneratedMessage with Message[E] with Defined[E] {

  self: E =>

  lazy val native: E = self

  val namekey: String

  def withNamekey(namekey: String): E

  def get(field: definition.FieldDefinition): field.V = {
    field.get(self)
  }

  def set(field: definition.FieldDefinition)(value: field.V): E = {
    field.set(self, value)
  }

  def id = definition.id(namekey)

  def eventBusID: EventBusEntityID = definition.eventBusID(namekey)

  def hydrateFieldValue[B<:BaseEntity[B]](implicit tag: ClassTag[B], ec: ExecutionContext): (Option[B],E)  =
      definition.nested.find(_.subentityDefinition.tag == tag) match{
        case Some(x: definition.NestedFieldDefinition) => {
          Try{x.getHydrated(self)} match {
            case Success(v) => (Some(v.asInstanceOf[B]), x.setHydrated(self, v))
            case _ => (None, self)
          }
        }
        case _ => (None, self)
      }

  def dehydrateFieldValue[B<:BaseEntity[B]](implicit tag: ClassTag[B], ec: ExecutionContext): (Option[String],E)  =
    definition.nested.find(_.subentityDefinition.tag == tag) match{
      case Some(x: definition.NestedFieldDefinition) => {
        Try{x.getScalar(self)} match {
          case Success(v) => (Some(v), x.setScalar(self, v))
          case _ => (None, self)
        }
      }
      case _ => (None, self)
    }

  def rehydrateFieldValue[B<:BaseEntity[B]](implicit tag: ClassTag[B], ec: ExecutionContext): (Option[B],E)  = dehydrateFieldValue[B]._2.hydrateFieldValue[B]

  def created: Date = {
    if (isInstanceOf[Audit]) {
      new Date(asInstanceOf[Audit].crt.millis)
    } else {
      implicit val ec = scala.concurrent.ExecutionContext.global
      hydrateFieldValue[Audit]._1 match {
        case Some(x: Audit) => x.created
        case _ => new Date(0)
      }
    }
  }

  def updated: Date = {
    if (isInstanceOf[Audit]) {
      new Date(asInstanceOf[Audit].upd.millis)
    } else {
      implicit val ec = scala.concurrent.ExecutionContext.global
      hydrateFieldValue[Audit]._1 match {
        case Some(x: Audit) => x.updated
        case _ => new Date(0)
      }
    }
  }

  def version: Int = {
    if (isInstanceOf[Audit]) {
      asInstanceOf[Audit].ver
    } else {
      implicit val ec = scala.concurrent.ExecutionContext.global
      hydrateFieldValue[Audit]._1 match {
        case Some(x: Audit) => x.version
        case _ => 0
      }
    }
  }

  def enforceAuditIfApplicable(scalar: Boolean = false) = {
    var needToGenerate: Boolean = false
    definition.nested.find(_.subentityDefinition.tag == AuditDef.tag) match {
      case Some(field) => {
        try {
          get(field) match {
            case x if (x.key != id.gkey.toString) => needToGenerate = true
            case x: KeyOneOf[_] if (!scalar) => needToGenerate = true
            case x: EntityOneOf[_] if scalar => needToGenerate = true
            case _ => {}
          }
        } catch {
          case x: Throwable => needToGenerate = true
        }
        if (needToGenerate){
          if (scalar) field.setScalar(self, id.gkey.toString) else
             field.setHydrated(self, field.subentityDefinition.newHollowInstance(id.gkey.toString))
        } else {
          self
        }
      }
      case _ => self
    }
  }

  def bumpUpAuditIfApplicable: E = {
    implicit val ct = definition.tag
    if (isInstanceOf[Audit]) {
      val a = asInstanceOf[Audit]
      val now = Timestamp.defaultInstance.withMillis(new Date().getTime)
      a.withUpd(now).withVer(a.ver+1).asInstanceOf[E]
    } else {
      implicit val ec = scala.concurrent.ExecutionContext.global
      definition.nested.find(_.subentityDefinition.tag == AuditDef.tag) match {
        case Some(x: definition.NestedFieldDefinition) => {
          Try {
            val audit = x.getHydrated(self)
            x.setHydrated(self, audit.bumpUpAuditIfApplicable)
          } match {
            case Success(v:E) => v
            case _ => self
          }
        }
        case _ => self
      }
    }
  }


}

trait Defined[E<:BaseEntity[E]] {
  val definition: EntityDefinition[E]
}


sealed trait OneOf[E<:BaseEntity[E]] {

  def etag: ClassTag[E]

  def definition: EntityDefinition[E]  = EntityDefinition[E](etag)

  def id: ID = definition.id(key)

  def eventBusID: EventBusEntityID = if (!key.isEmpty) definition.eventBusID(key) else definition.categoryID

  def isHydrated: Boolean

  def key: String

  def entity: Option[E]

}

final case class KeyOneOf[E<:BaseEntity[E]](namekey: String)(implicit tag: ClassTag[E]) extends OneOf[E] {

  override def etag = tag

  override def key: String = namekey

  override def isHydrated: Boolean = false

  override def entity: Option[E] = None
}


final case class EntityOneOf[E<:BaseEntity[E]] (value: E) extends OneOf[E] {

  override def etag = value.definition.tag

  override def definition = value.definition

  override def key: String = value.namekey

  override def isHydrated: Boolean = true

  override def entity: Option[E] = Some(value)
}

object EntityDefinition {

  import scala.language.existentials

  private val registry = new ConcurrentMapSimpleRegistry[EntityDefinition[E] forSome {type E<:BaseEntity[E]}](
    Reg.cbBuilder[EntityDefinition[E] forSome {type E<:BaseEntity[E]}].build())

  private val edParentID = ID(ID.UtilityCategory, "ED")

  private def id[E<:BaseEntity[E]](implicit ct: ClassTag[E]): ID = {
    ID(edParentID, ct.runtimeClass.toString)
  }

  def register[E<:BaseEntity[E]](entityDefinition: EntityDefinition[E])(implicit ct: ClassTag[E]): Unit = {
    registry ++ (Reg.Add[EntityDefinition[E] forSome {type E<:BaseEntity[E]}], id(ct), entityDefinition)
  }

  @throws[ClassNotFoundException]
  def apply[E <: BaseEntity[E]](implicit ct: ClassTag[E]): EntityDefinition[E] = {
    registry ? (Reg.Get[EntityDefinition[E] forSome {type E<:BaseEntity[E]}], id(ct)) match {
      case Success(Some(x: EntityDefinition[_])) if x.tag == ct => {
        x.asInstanceOf[EntityDefinition[E]]
      }
      case _ => throw new ClassNotFoundException(s"Failed to find an entity definition class for entity ${ct.runtimeClass}")
    }
  }

  def all(): Set[EntityDefinition[E] forSome {type E<:BaseEntity[E]}] = {
    registry.outer.regContext.registeredItems.values.toSet
  }

}

trait EntityDefinition[E<:BaseEntity[E]] {

  def tag: ClassTag[E]

  def newHollowInstance(namekey: String = ID(categoryID).nameKey) = {
    val v = companion.defaultInstance
    v.set(v.definition.Namekey)(namekey)
  }

  def categoryID: NameKeyID with EventBusEntityID

  def companion: GeneratedMessageCompanion[E]

  def id(namekey: String) = ID(categoryID, namekey)

  def eventBusID(namekey: String): EventBusEntityID = EventBusEntityID(categoryID,namekey)

  val crud: CRUD[E]

  val rest: REST[E]

  val io: IO[E]

  val mq: MQ[E]

  def fields: Set[this.FieldDefinition]

  val Namekey = new this.NameKeyFieldDefinition{}

  def nested: Set[this.NestedFieldDefinition] = fields.filter(_.isInstanceOf[NestedFieldDefinition]).map(_.asInstanceOf[NestedFieldDefinition])

  sealed trait FieldDefinition {

    type V<:Any

    val vtag: ClassTag[V]

    val descriptor: Descriptors.FieldDescriptor

    def get(entity: E): V

    def set(entity: E, value: V): E

  }

  trait SimpleFieldDefinition extends FieldDefinition{

    def get(entity: E): V = {
      entity.getField(descriptor).asInstanceOf[V]
    }

  }

  trait NameKeyFieldDefinition extends SimpleFieldDefinition{
    override type V = String
    override val vtag = ClassTag[String](classOf[String])
    override val descriptor = companion.descriptor.findFieldByName("namekey")
    override def set(entity: E, value: String): E = {
      entity.withNamekey(value).enforceAuditIfApplicable(true).enforceAuditIfApplicable(false)
    }
  }

  trait EnumFieldDefinition extends FieldDefinition {

    type V <:GeneratedEnum

    def getEnumValue(value: String): Option[V] = {
      try {
        val v = companion.enumCompanionForField(descriptor).fromName(value)
        v match {
          case Some(x) if vtag.runtimeClass.isAssignableFrom(x.getClass()) => Some(x.asInstanceOf[V])
          case _ => None
        }
      } catch {
        case x: Throwable => None
      }
    }

  }

  trait NestedFieldDefinition extends FieldDefinition {


    type Sub<:BaseEntity[Sub]

    type V = OneOf[Sub]

    override val vtag = ClassTag[V](classOf[V])

    val subentityDefinition: EntityDefinition[Sub]

    def getHydrated(entity: E)(implicit ec: ExecutionContext): Sub =
      get(entity) match {
        case x: EntityOneOf[Sub] => x.entity.get
        case y: KeyOneOf[Sub] => {
          Await.result(subentityDefinition.crud.read(y.namekey), 5.seconds).get
        }
      }
    def getScalar(entity: E): String =
      get(entity) match {
        case x: EntityOneOf[Sub] => x.entity.get.namekey
        case y: KeyOneOf[Sub] => y.namekey
      }
    def setHydrated(entity: E, value: Sub): E = {
      set(entity, EntityOneOf[Sub](value))
    }
    def setScalar(entity: E, value: String): E = {
      set(entity, KeyOneOf[Sub](value)(subentityDefinition.tag))
    }
  }

  trait OptionalSimpleFieldDefinition extends FieldDefinition{
    type Value <:Any
    override type V = Option[Value]
    override val vtag = ClassTag[V](classOf[Option[V]])
  }

  trait OptionalEnumFieldDefinition extends FieldDefinition{
    type Value <:GeneratedEnum
    val enumTag: ClassTag[Value]

    def getEnumValue(value: String): Option[Value] = {
      try {
        val v = companion.enumCompanionForField(descriptor).fromName(value)
        v match {
          case Some(x) if vtag.runtimeClass.isAssignableFrom(x.getClass()) => Some(x.asInstanceOf[Value])
          case _ => None
        }
      } catch {
        case x: Throwable => None
      }
    }
  }

  trait OptionalNestedFieldDefinition extends FieldDefinition{

    type Sub<:BaseEntity[Sub]

    val stag: ClassTag[Sub]

    val subentityDefinition: EntityDefinition[Sub]

    override type V = Option[OneOf[Sub]]

    override val vtag = ClassTag[V](classOf[V])

    def getHydrated(entity: E)(implicit context: BagpipePostgreSQLContext, ec: ExecutionContext): Option[Sub] =
      get(entity) match {
        case Some(x: EntityOneOf[Sub]) => Some(x.entity.get)
        case Some(y: KeyOneOf[Sub]) => Some(Await.result(subentityDefinition.crud.read(y.namekey), scala.concurrent.duration.Duration.Inf).get)
        case _ => None
      }
    def getScalar(entity: E): Option[String] =
      get(entity) match {
        case Some(x: EntityOneOf[Sub]) => Some(x.entity.get.namekey)
        case Some(y: KeyOneOf[Sub]) => Some(y.namekey)
        case _ => None
      }
    def setHydrated(entity: E, value: Sub): E = {
      set(entity, Some(EntityOneOf[Sub](value)))
    }
    def setScalar(entity: E, value: String): E = {
      set(entity, Some(KeyOneOf[Sub](value)(subentityDefinition.tag)))
    }
  }


}

