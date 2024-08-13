/*
 *
 * Author: Andrew Torson
 * Date: Nov 16, 2016
 */

package net.andrewtorson.bagpipe.eventbus

import scala.reflect.ClassTag

import net.andrewtorson.bagpipe.utils._

/**
 * Created by Andrew Torson on 11/16/16.
 */

object EventBusClassifiers {



  // commands
  val CREATE = EventBusMessageID(EventBusMessageID.MessageCategory, "CRT")
  val UPDATE = EventBusMessageID(EventBusMessageID.MessageCategory, "UPD")
  val READ = EventBusMessageID(EventBusMessageID.MessageCategory, "GET")
  val DELETE = EventBusMessageID(EventBusMessageID.MessageCategory, "DEL")
  val POLL = EventBusMessageID(EventBusMessageID.MessageCategory, "POLL")

  // results
  val CREATED = EventBusMessageID(EventBusMessageID.MessageCategory, "CRTD")
  val UPDATED = EventBusMessageID(EventBusMessageID.MessageCategory, "UPDTD")
  val RETRIEVED = EventBusMessageID(EventBusMessageID.MessageCategory, "GOT")
  val DELETED = EventBusMessageID(EventBusMessageID.MessageCategory, "DELTD")
  val MISSING = EventBusMessageID(EventBusMessageID.MessageCategory, "MISS")
  val FAILED = EventBusMessageID(EventBusMessageID.MessageCategory, "FAILD")
  val POLLED = EventBusMessageID(EventBusMessageID.MessageCategory, "POLLD")


  //services
  val DB = EventBusMessageID(EventBusMessageID.MessageCategory, "DB")
  val REST = EventBusMessageID(EventBusMessageID.MessageCategory, "REST")
  val TCP = EventBusMessageID(EventBusMessageID.MessageCategory, "TCP")
  val MQ = EventBusMessageID(EventBusMessageID.MessageCategory, "MQ")
  val INTERNAL = EventBusMessageID(EventBusMessageID.MessageCategory, "INT")

  //TCP service verbs
  val TCP_CREATE = EventBusMessageID(TCP, CREATE)
  val TCP_UPDATE = EventBusMessageID(TCP, UPDATE)
  val TCP_CREATED = EventBusMessageID(TCP, CREATED)
  val TCP_UPDATED = EventBusMessageID(TCP, UPDATED)

  //REST service verbs
  val REST_CREATE = EventBusMessageID(REST, CREATE)
  val REST_UPDATE = EventBusMessageID(REST, UPDATE)
  val REST_READ = EventBusMessageID(REST, READ)
  val REST_DELETE = EventBusMessageID(REST, DELETE)
  val REST_POLL = EventBusMessageID(REST, POLL)
  val REST_CREATED = EventBusMessageID(REST, CREATED)
  val REST_UPDATED = EventBusMessageID(REST, UPDATED)


  //DB service verbs
  val DB_CREATED = EventBusMessageID(DB, CREATED)
  val DB_UPDATED = EventBusMessageID(DB, UPDATED)
  val DB_RETRIEVED = EventBusMessageID(DB, RETRIEVED)
  val DB_DELETED = EventBusMessageID(DB, DELETED)
  val DB_MISSING = EventBusMessageID(DB, MISSING)
  val DB_FAILURE = EventBusMessageID(DB, FAILED)
  val DB_POLLED = EventBusMessageID(DB, POLLED)

  //MQ service verbs
  val MQ_CREATE = EventBusMessageID(MQ, CREATE)
  val MQ_UPDATE = EventBusMessageID(MQ, UPDATE)
  val MQ_CREATED = EventBusMessageID(MQ, CREATED)
  val MQ_UPDATED = EventBusMessageID(MQ, UPDATED)

  //INTERNAL service verbs
  val INTERNAL_POLL = EventBusMessageID(INTERNAL, POLL)
  val INTERNAL_CREATE = EventBusMessageID(INTERNAL, CREATE)
  val INTERNAL_UPDATE = EventBusMessageID(INTERNAL, UPDATE)



  def topic(eventIDs: EventBusMessageID*)(entityID: EventBusEntityID): Seq[EventBusTopicID] = for (eventID<-eventIDs) yield EventBusTopicID(eventID, entityID)

  def apply[E<:BaseEntity[E]](eventId: EventBusMessageID, predicate: OneOf[E] => Boolean, categoryLevel: Boolean = true): EntityClassifier[E] =
    if (categoryLevel)  SimpleEntityCategoryClassifier[E](eventId, predicate) else SimpleEntityClassifier[E](eventId, predicate)

  final case class SimpleEntityClassifier[E<:BaseEntity[E]](eventId: EventBusMessageID, predicate: OneOf[E] => Boolean) extends EntityClassifier[E]{
    override def classify(value: OneOf[E]): EventBusTopicID = if (predicate(value)) topic(eventId)(value.eventBusID)(0) else
      throw new IllegalArgumentException(s"Value $value can't be classified as $eventId")
  }

  final case class SimpleEntityCategoryClassifier[E<:BaseEntity[E]](eventId: EventBusMessageID, predicate: OneOf[E] => Boolean) extends EntityClassifier[E]{
    override def classify(value: OneOf[E]): EventBusTopicID = if (predicate(value)) topic(eventId)(value.definition.categoryID)(0) else
      throw new IllegalArgumentException(s"Value $value can't be classified as $eventId")
  }

  def entityPredicate[E<:BaseEntity[E]](): (OneOf[E] => Boolean) = {e: OneOf[E] => {
    implicit val etag: ClassTag[E] = e.etag
    e match {
      case EntityOneOf(value: E) => true
      case _ => false
    }
  }}

  def trivialPredicate[E<:BaseEntity[E]](): (OneOf[E] => Boolean) = {e: OneOf[E] => {true}}

}

