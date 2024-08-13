/*
 *
 * Author: Andrew Torson
 * Date: Jan 24, 2017
 */

package net.andrewtorson.bagpipe.networking

import java.util.Date

import scala.reflect.ClassTag
import scala.util.{Success, Try}

import akka.stream.scaladsl.{Flow, Sink}
import com.google.protobuf.Descriptors.FieldDescriptor
import net.andrewtorson.bagpipe.{ApplicationService, ApplicationServiceUnavailableException}
import net.andrewtorson.bagpipe.utils.{ID, _}
import com.trueaccord.scalapb.{GeneratedMessage, GeneratedMessageCompanion}
import net.andrewtorson.bagpipe.eventbus.{EntityBusEvent, EventBusClassifiers, EventBusEntityID, EventBusMessageID}
import scala.language.existentials

import akka.stream.{ActorAttributes, Supervision}
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType
import net.andrewtorson.bagpipe.networking.IO.NetworkingProtocol




/**
 * Created by Andrew Torson on 1/24/17.
 */

object IO {

  import net.andrewtorson.bagpipe.eventbus.EventBusClassifiers._

  sealed trait NetworkingProtocol{
    val messageID: EventBusMessageID
  }

  case object TCP_PROTOCOL extends NetworkingProtocol {
    override val messageID: EventBusMessageID = TCP
  }

  case object REST_PROTOCOL extends NetworkingProtocol {
    override val messageID: EventBusMessageID = REST
  }

  case object MQ_PROTOCOL extends NetworkingProtocol {
    override val messageID: EventBusMessageID = MQ
  }

}

trait IO[E<:BaseEntity[E]]{

  type Repr = (E, Option[String])

  import net.andrewtorson.bagpipe.eventbus.EventBusClassifiers._

  protected val entityDefinition: EntityDefinition[E]

  @throws(classOf[ApplicationServiceUnavailableException])
  protected def serviceBus = ApplicationService[ServiceBusModule]

  def ioFlow(protocol: NetworkingProtocol): Flow[Repr, Repr,_] = {
    val source = serviceBus.source(topic(EventBusMessageID(protocol.messageID, CREATED), EventBusMessageID(protocol.messageID,UPDATED))(entityDefinition.categoryID):_*)()._2
    Flow.fromSinkAndSource[Repr, Repr](Flow[Repr].filter(!_._1.namekey.isEmpty).map(e => {
      val v = e._1.enforceAuditIfApplicable(false)
      val ctx = e._2
      val correlationID = ctx.map(ID(EntityBusEvent.baseCorrelationID,_)).getOrElse(ID(EntityBusEvent.baseCorrelationID))
      v.version match {
      case x if (x > 1) =>   EntityBusEvent[E](EntityOneOf(v), EventBusClassifiers(EventBusMessageID(protocol.messageID,UPDATE), entityPredicate()), v.updated, correlationID)(entityDefinition.tag)
      case _ =>   EntityBusEvent[E](EntityOneOf(v), EventBusClassifiers(EventBusMessageID(protocol.messageID,CREATE), entityPredicate()), v.created, correlationID)(entityDefinition.tag)
    }}).to(Sink.foreach(serviceBus.deliver(_))), source.collect[Repr]{case x: EntityBusEvent if x.value.entity.isDefined =>
      (x.value.entity.get.asInstanceOf[E].enforceAuditIfApplicable(false), x.correlationID.nameKey.startsWith(ID.randomPrefix) match {
        case false => Some(x.correlationID.nameKey)
        case _ => None
      })})
    .withAttributes(ActorAttributes.supervisionStrategy(Supervision.resumingDecider))
  }

}


trait WrappedEntity extends GeneratedMessage{

  val entityCategory: String

  @throws[ClassCastException]
  def getEntity(): BE = WrappedEntity.getEntity(this)

  @throws[ClassCastException]
  def setEntity(entity: BE): WrappedEntity = WrappedEntity.getWrapper(entity)(this)

}


object WrappedEntity {


  import collection.JavaConverters._

  protected val descriptorCache = new ConcurrentMapSimpleRegistry[FieldDescriptor](Reg.cbBuilder[FieldDescriptor].build())

  protected lazy val entityDefinitions: Map[String, EntityDefinition[E] forSome {type E <: BaseEntity[E]}] = EntityDefinition.all().groupBy(_.categoryID.nameKey).mapValues(_.head)

  protected def fromClassTag[E <: BaseEntity[E]](entity: Any)(implicit ct: ClassTag[E]): BaseEntity[E] = {
    if (ct.runtimeClass.isInstance(entity))
      Try {
        entity.asInstanceOf[BaseEntity[E]]
      } match {
        case Success(e: E) => e
        case _ => throwClassCastException(entity)
      } else throwClassCastException(entity)
  }

  protected def getEntityDescriptor(entityDefinition: EntityDefinition[_], wrapperCompanion: GeneratedMessageCompanion[_], onFailure: => Throwable): FieldDescriptor = {

  descriptorCache ? (Reg.GetOrAdd[FieldDescriptor], entityDefinition.categoryID,
    Some({ () => Some(iterableAsScalaIterableConverter(wrapperCompanion.descriptor.getFields()).asScala.filter(_.getJavaType == JavaType.MESSAGE).find(
      wrapperCompanion.messageCompanionForField(_).descriptor.getFullName == entityDefinition.companion.descriptor.getFullName).get)
    })) match {
    case Success(Some(fd)) => fd
    case _ => throw onFailure
  }
 }

  protected def getCategoryDescriptor(wrapper: WrappedEntity, onFailure: => Throwable): FieldDescriptor =
    descriptorCache ? (Reg.GetOrAdd[FieldDescriptor], EventBusEntityID.EntityCategory,
      Some({() => Some(wrapper.getAllFields.find(_._2 == wrapper.entityCategory).get._1)})) match {
      case Success(Some(fd)) => fd
      case _ => throw onFailure
    }

  private def throwClassCastException(entity: Any)(implicit ct: ClassTag[_]): Nothing = throw new ClassCastException(s"Failed to cast $entity to ${ct.runtimeClass}")

  private def throwExtractException(wrappedEntity: WrappedEntity): Nothing = throw new ClassCastException(s"Failed to unwrap an instance of ${wrappedEntity.entityCategory} from $wrappedEntity")

  @throws[ClassCastException]
  def getEntity(wrappedEntity: WrappedEntity) =
    entityDefinitions.get(wrappedEntity.entityCategory) match {
      case Some(definition) =>  {
        fromClassTag(wrappedEntity.getField(getEntityDescriptor(definition, wrappedEntity.companion, throwExtractException(wrappedEntity))))(definition.tag)
      }
      case _ => throwExtractException(wrappedEntity)
    }

  @throws[ClassCastException]
  def getWrapper(entity: BE)(wrapper: WrappedEntity) = {
    val definition = entity.definition
    wrapper.companion.fromFieldsMap(wrapper.getAllFields
      + (getCategoryDescriptor(wrapper,throwExtractException(wrapper)) -> definition.categoryID.nameKey)
      + (getEntityDescriptor(definition, wrapper.companion, throwExtractException(wrapper)) -> entity)).asInstanceOf[WrappedEntity]
  }

}
