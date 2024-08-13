/*
 *
 * Author: Andrew Torson
 * Date: Nov 14, 2016
 */

package net.andrewtorson.bagpipe.eventbus


import java.util.{Date, UUID}
import java.util.concurrent.Executors

import scala.annotation.tailrec
import scala.concurrent._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}
import scala.reflect.ClassTag
import scala.util.Success
import akka.Done
import akka.actor.ActorRef
import akka.event._
import akka.util.Subclassification
import net.andrewtorson.bagpipe.streaming.BackpressuredActorPublisherProxy
import net.andrewtorson.bagpipe.streaming.StreamingFlowOps.DelayedActorExecution
import net.andrewtorson.bagpipe.utils.{BaseEntity, _}

/**
 * Created by Andrew Torson on 11/14/16.
 */


object EntityEventBus extends ConcurrentMapSimpleRegistry[R](Reg.cbBuilder[R].build()) with EntityEventBus{

}

sealed trait EventBusID extends ID {

  final protected def findAncestors(categoryID: CategoryID): Set[ID] = {
    findAncestor(Seq[ID](categoryID), true) match{
      case Some(x) => stripCompanions(x, categoryID)
      case _ =>  Set.empty[ID]
    }
  }

  @tailrec final protected def stripCompanions(id: ID, category: CategoryID): Set[ID] = {
    (id.parentCompanionID, id.parentID) match {
      case (Some(x), y) if (y != id) => {
        if (x.findAncestor(Seq[ID](category)).isEmpty) stripCompanions(y, category) else Set[ID](y,x)
      }
      case _ => Set[ID](id)
    }
  }

  final protected def checkNonEmpty(ids: Set[ID], category: CategoryID): Set[ID] = {
    require(!ids.isEmpty, s"Event bus ID ${this} must have non-empty ancestors in the $category category")
    ids
  }

}

object EventBusMessageID {

  def apply(parentID: EventBusMessageID, nameKeyID: String): EventBusMessageID = new NameKeyID(parentID, nameKeyID) with EventBusMessageID

  def apply(parentID: EventBusMessageID, parentCompanionID: ID): EventBusMessageID = {
    require(!parentCompanionID.isInstanceOf[EventBusEntityID] && !parentCompanionID.isInstanceOf[EventBusTopicID],
      "Can't cast event bus topic ID as an event bus message ID")
    new RelationID(parentID, parentCompanionID) with EventBusMessageID
  }

  def apply(parentID: EventBusMessageID): EventBusMessageID   = apply(parentID, UUID.randomUUID().toString)

  val MessageCategory = new CategoryID(ID.MessageCategory.categoryName) with EventBusMessageID

}

trait EventBusMessageID extends EventBusID {

  lazy val messageCategoryAncestors: Set[ID] = if (this.gkey == ID.MessageCategory.gkey) Set[ID](this) else checkNonEmpty((this.parentID, this.parentCompanionID) match {
    case (p: EventBusMessageID, None) => Set[ID](this)
    case (p: EventBusMessageID, Some(c: EventBusMessageID)) => Set[ID](p,c)
    case (p: EventBusMessageID, _) => p.messageCategoryAncestors
    case (p: EventBusEntityID , Some(c: EventBusMessageID)) => c.messageCategoryAncestors
    case _ => findAncestors(ID.MessageCategory)
  }, ID.MessageCategory)

  def isMessageSub(y: EventBusMessageID): Boolean =
    if ((y.messageCategoryAncestors diff this.messageCategoryAncestors).isEmpty) true else if (this.gkey == ID.MessageCategory.gkey) false else
      (this.parentID, this.parentCompanionID) match {
        case (p: EventBusMessageID, None) =>  p.isMessageSub(y)
        case (p: EventBusMessageID, Some(c: EventBusMessageID)) => if (y.messageCategoryAncestors.size == 1)
          {p.isMessageSub(y) || c.isMessageSub(y)} else {this.findAncestor(y.messageCategoryAncestors.toSeq).isDefined}
        case (p: EventBusMessageID, _) => p.isMessageSub(y)
        case (p: EventBusEntityID , Some(c: EventBusMessageID)) => c.isMessageSub(y)
        case _ => false
      }

}

object EventBusEntityID {

  def apply(parentID: EventBusEntityID, nameKeyID: String) = new NameKeyID(parentID, nameKeyID) with EventBusEntityID

  def apply(parentID: EventBusEntityID, parentCompanionID: ID): EventBusEntityID = {
    require(!parentCompanionID.isInstanceOf[EventBusMessageID] && !parentCompanionID.isInstanceOf[EventBusTopicID],
      "Can't cast event bus topic ID as an event bus entity ID")
    new RelationID(parentID, parentCompanionID) with EventBusEntityID
  }

  def apply(parentID: EventBusEntityID): EventBusEntityID   = apply(parentID, UUID.randomUUID().toString)

  val EntityCategory = new CategoryID(ID.EntityCategory.categoryName) with EventBusEntityID

}

trait EventBusEntityID extends EventBusID {
  lazy val entityCategoryAncestors: Set[ID] = if (this.gkey == ID.EntityCategory.gkey) Set[ID](this) else checkNonEmpty((this.parentID, this.parentCompanionID) match {
    case (p: EventBusEntityID, None) => Set[ID](this)
    case (p: EventBusEntityID, Some(c: EventBusEntityID)) => Set[ID](p,c)
    case (p: EventBusEntityID, _) => p.entityCategoryAncestors
    case (p: EventBusMessageID , Some(c: EventBusEntityID)) => c.entityCategoryAncestors
    case _ => findAncestors(ID.EntityCategory)
  }, ID.EntityCategory)

  def isEntitySub(y: EventBusEntityID): Boolean =
    if ((y.entityCategoryAncestors diff this.entityCategoryAncestors).isEmpty) true else if (this.gkey == ID.EntityCategory.gkey) false else
      (this.parentID, this.parentCompanionID) match {
        case (p: EventBusEntityID, None) =>  p.isEntitySub(y)
        case (p: EventBusEntityID, Some(c: EventBusEntityID)) => if (y.entityCategoryAncestors.size == 1)
              {p.isEntitySub(y) || c.isEntitySub(y)} else {this.findAncestor(y.entityCategoryAncestors.toSeq).isDefined}
        case (p: EventBusEntityID, _) => p.isEntitySub(y)
        case (p: EventBusMessageID , Some(c: EventBusEntityID)) => c.isEntitySub(y)
        case _ => false
      }

}

object EventBusTopicID {

  def apply(parentID: EventBusTopicID, nameKeyID: String): EventBusTopicID = new NameKeyID(parentID, nameKeyID) with EventBusTopicID

  def apply(parentID: EventBusEntityID, parentCompanionID: EventBusMessageID): EventBusTopicID = {
    new RelationID(parentID, parentCompanionID) with EventBusTopicID
  }

  def apply(parentID: EventBusMessageID, parentCompanionID: EventBusEntityID): EventBusTopicID = {
    new RelationID(parentID, parentCompanionID) with EventBusTopicID
  }

  def apply(parentID: EventBusTopicID): EventBusTopicID   = apply(parentID, UUID.randomUUID().toString)

}

trait EventBusTopicID extends EventBusMessageID with EventBusEntityID{


  def isEqual(x: EventBusTopicID): Boolean = (this.equals(x))

  def isSub(y: EventBusTopicID): Boolean = (isMessageSub(y) && isEntitySub(y))

}


trait EntityEventBus extends EventBus with SubchannelClassification{

  //ToDo: need an index clean-up job set-up to remove keys that have empty set of subscribers and all their non-root children have empty subsrciber sets too
  self: SimpleRegistryFacade[R] =>

  override type Event = EntityBusEvent
  override type Classifier = EventBusTopicID
  override type Subscriber = EntityBusSubscriber

  override protected def classify(event: Event): EventBusTopicID = event.topic

  protected implicit val subclassification = EventBusTopicClassification

  //ToDO: may need to speed up these methods by defining a trait on top of IDs that would provide entity and message category mathcing based on extra construction-time fields
  // it will speed up new subscriptions and cash-miss cases
  protected object EventBusTopicClassification extends Subclassification[Classifier]{
    def isEqual(x: Classifier, y: Classifier) = x.isEqual(y)

    def isSubclass(x: Classifier, y: Classifier) = x.isSub(y)
  }

  override def subscribe(subscriber: Subscriber, to: Classifier): Boolean = {
    val result = super.subscribe(subscriber, to)
    result && (self >>(Reg.Wrap(Reg.Get[R,Unit] - Reg.IfAbsent[R,Unit].-<(Reg.Add[R,Unit],
      Reg.Map[R,Unit]{x: R => (x._1, x._2 + to)} - Reg.Replace[R,Unit])), subscriber.subscriberID, Some({() => Some((subscriber, Set[ID](to)))})))
  }

  override def unsubscribe(subscriber: Subscriber, from: Classifier): Boolean = {
    val result = self >>(Reg.Wrap(Reg.Get[R,Unit] - Reg.IfAbsent[R,Unit].-<(Reg.Nil[R,Unit],
      Reg.Map[R,Unit]{x: R => (x._1, x._2 - from)} - Reg.Replace[R,Unit])), subscriber.subscriberID, Some({() => None}))
    result && super.unsubscribe(subscriber, from)
  }

  override def unsubscribe(subscriber: EntityBusSubscriber): Unit = {
    self -- (Reg.Remove[R], subscriber.subscriberID)
    super.unsubscribe(subscriber)
  }

  def unsubscribeID(subscriberID: ID): Unit = {
    self >> (Reg.Wrap(Reg.Get[R,Unit] - Reg.IfAbsent[R,Unit].-<(Reg.Nil[R,Unit],
      Reg.Callable[R,Unit]{x: R => super.unsubscribe(x._1)} - Reg.Remove[R,Unit])), subscriberID, Some({() => None}))
  }

  def getCurrentSubscriptions(subscriberID: ID): Option[R] = {
    self ? (Reg.Get[R], subscriberID) match {
      case Success(x) => x
      case _ => None
    }
  }

  override protected def publish(event: Event, subscriber:  Subscriber) = {
    subscriber.executeItem(event)
  }

}


trait EntityClassifier[E<:BaseEntity[E]]{

  def classify(value: OneOf[E]): EventBusTopicID

}


trait EntityBusEvent {

    val timestamp: Date

    val correlationID: NameKeyID

    type E<:BaseEntity[E]

    val value: OneOf[E]

    val classifier: EntityClassifier[E]

    val topic: EventBusTopicID = classifier.classify(value)
}

object EntityBusEvent {

  val baseCorrelationID = ID(ID.UtilityCategory, "EBCR")

  def apply[E<:BaseEntity[E]](value: OneOf[E], classifier: EntityClassifier[E], timestamp: Date = new Date(), correlationID: NameKeyID = ID(baseCorrelationID))(implicit ct: ClassTag[E]): EntityBusEvent
  =  new MixingEntityBusEvent[E](value, classifier, timestamp, correlationID)

  final case class MixingEntityBusEvent[B<:BaseEntity[B]: ClassTag](
    override val value: OneOf[B],
    override val classifier: EntityClassifier[B],
    override val timestamp: Date,
    override val correlationID: NameKeyID) extends EntityBusEvent{
    override type E = B
  }
}

object EntityBusSubscriber {

  type S = BackpressuredActorPublisherProxy
  type T = EntityBusEvent

  // Async vs. sync: async subscription breaks in-flow back-pressure (but custom ExecutorContext may backpressure globally if it is overflown by blocking 'submit')
  // async guarantees that in case of multiple matching subscribers there will never be no Head-of-the-Line blocking (by one slow subscriber).
  // Note that HOL will always happen for a single publish() call within a line of matching subscribers if sync is used.
  // But, if multiple publish() calls happen on the same thread (bad! people should always publish multi-threaded!) then they will be all affected by HOL is sync is used
  // sync preserves in-flow backpressure through
  // Also, there may be really complex locking situations with sync: HOL on a single publish call in a multi-threaded env may sill globally block if the subscriber held up is a global one (i.e. being matched by many publish call)
  // global subscribers (such as background DB service) should be async! and their backpressure should be managed through ThreadPool overflow
  // also, global subscribers internally should be really careful with using publish as a response (e.g. call publish on a separate thread pool) because it may block (if there is a sync consumer)
  // Advice: use sync if there is a single producer-consumer channel (i.e. Queue pattern) and publishing is multi-threaded (e.g. in a Flow that is either isolated or is global and has async boundary)
  def apply(subscriberID: ID, ordered: Boolean = true, async: Boolean = false)(implicit ec: ExecutionContext): EntityBusSubscriber =
    (async, ordered) match {
      case (true, true) => new EntityBusSubscriber(subscriberID, ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor()), true, true) // ToDO: need to define a global sticky execution context and use it here
      case (true, false) => new EntityBusSubscriber(subscriberID, ec, false, true)
      case  _ => new EntityBusSubscriber(subscriberID, ec, false, false)
    }

  protected def asyncSubscriberCallback(subscriberID: ID, ec: ExecutionContext, manageContext: Boolean): ((Option[S], T, ActorRef) => Option[S])  = {
    (state: Option[S], item: T, routee: ActorRef) => {
      state match {
        case None => {
          val proxy = new BackpressuredActorPublisherProxy(routee, subscriberID, scala.concurrent.duration.Duration.Inf)
          asyncSend(proxy, item, ec)
          Some(proxy)
        }
        case Some(proxy) => {
          asyncSend(proxy, item,ec)
          state
        }
      }
    }
  }

  protected def syncSubscriberCallback(subscriberID: ID): ((Option[S], T, ActorRef) => Option[S])  = {
    (state: Option[S], item: T, routee: ActorRef) => {
      state match {
        case None => {
          val proxy = new BackpressuredActorPublisherProxy(routee, subscriberID, scala.concurrent.duration.Duration.Inf)
          syncSend(proxy, item)
          Some(proxy)
        }
        case Some(proxy) => {
          syncSend(proxy, item)
          state
        }
      }
    }
  }


  protected def asyncSend(proxy: BackpressuredActorPublisherProxy, item: EntityBusEvent, ec: ExecutionContext): Future[Done] =  Future{
    blocking {
      if (proxy.syncSend(item)) Done else throw new IllegalStateException("Destination actor has been terminated")
    }
  }(ec)

  protected def syncSend(proxy: BackpressuredActorPublisherProxy, item: EntityBusEvent): Unit =
   if (!proxy.syncSend(item)) throw new IllegalStateException("Destination actor has been terminated")

}

class EntityBusSubscriber(val subscriberID: ID, ec: ExecutionContext, manageContext: Boolean, async: Boolean) extends DelayedActorExecution[BackpressuredActorPublisherProxy, EntityBusEvent](
  if (async) EntityBusSubscriber.asyncSubscriberCallback(subscriberID, ec, manageContext) else EntityBusSubscriber.syncSubscriberCallback(subscriberID), true){
  override def setActor(actor: ActorRef): Boolean = {
    this.synchronized {
      internalActor match {
        case None => {
          actor ! TerminateCallback(false, { () => {
            ec match {
              case x: ExecutionContextExecutorService => if (manageContext) x.shutdown()
              case _ => {}
            }
            EntityEventBus.unsubscribeID(subscriberID)
          }
          })
        }
        case _ => {}
      }
    }
    super.setActor(actor)
  }
}