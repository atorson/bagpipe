/*
 *
 * Author: Andrew Torson
 * Date: Aug 26, 2016
 */

package net.andrewtorson.bagpipe.streaming





import scala.annotation.tailrec
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import scala.reflect.ClassTag

import akka.Done
import akka.actor.{ActorLogging, ActorRef, Props}
import akka.stream.actor.ActorPublisher
import net.andrewtorson.bagpipe.utils._



/**
 * Created by Andrew Torson on 8/26/16.
 */



case object Authorized
case object Terminal
case class Abort(exc: Throwable)


class BackpressuredActorPublisherProxy(actor: ActorRef, id: ID, timeout: Duration){

  private var backPressure = false
  private var reliefPromise: Promise[Done] = Promise.successful(Done)
  private var registered = false
  private val facade = new ActorBasedSimpleRegistryFacade[C](actor)

  if (!registered) {
    facade ++ (Reg.Add[C], id,
      (() => {
        backPressure = true
        reliefPromise = Promise[Done]
      }, () => {
        backPressure = false
        if (!reliefPromise.isCompleted) reliefPromise success (Done)
      }))
    registered = true
  }


  def terminate(): Unit = {
    if (registered) {
      facade -- (Reg.Remove[C], id)
      registered = false
    }
  }

  def syncSend(message: Any): Boolean = {
    if (facade.isAlive()) {
      if (backPressure) Await.result(reliefPromise.future, timeout)
      actor ! message
      true
    } else {false}
  }

  def asyncSend(message: Any): Option[Future[Done]] = {
    if (facade.isAlive()) {
      if (backPressure) Some(reliefPromise.future)
      else {
        actor ! message
        None
      }
    } else {None}
  }



}



object StreamingPublisherActor{

  def props[T]()(implicit tag: ClassTag[T]): Props = props[T](None, None, true, false)

  def props[T](flowActorsRegistry: Option[AR],  flowActorId: Option[ID],  preAuthorized: Boolean, stopOnTerminalIfNotAuthorized: Boolean)(implicit tag: ClassTag[T]): Props =
    props[T](flowActorsRegistry, flowActorId, preAuthorized, stopOnTerminalIfNotAuthorized, 50, 50, 0.6F, 0.2F)

  def props[T](flowActorsRegistry: Option[AR],  flowActorId: Option[ID],  preAuthorized: Boolean, stopOnTerminalIfNotAuthorized: Boolean,
    initialBufferSize:Int, bufferSizeIncrementPerChannel: Int, highWaterMarkPercentage: Float, lowWaterMarkPercentage: Float)(implicit tag: ClassTag[T]): Props =
    Props (new StreamingPublisherActor[T](flowActorsRegistry, flowActorId, preAuthorized, stopOnTerminalIfNotAuthorized,
      initialBufferSize, bufferSizeIncrementPerChannel, highWaterMarkPercentage, lowWaterMarkPercentage))

}

class StreamingPublisherActor[T](flowActorsRegistry: Option[AR],
  flowActorId: Option[ID],
  preAuthorized: Boolean,
  val stopOnTerminalIfNotAuthorized: Boolean,
  initialBufferSize: Int,
  bufferSizeIncrementPerChannel: Int,
  highWaterMarkPercentage: Float,
  lowWaterMarkPercentage: Float)(implicit tag: ClassTag[T])
  extends SimpleRegistryActor[C](Reg.cbBuilder[C].build()) with ActorPublisher[T] with ActorLogging {

  case object QueueUpdated

  import akka.stream.actor.ActorPublisherMessage._
  import scala.collection.mutable


  require(initialBufferSize > 1, "Initial buffer size must be at least two")
  require(bufferSizeIncrementPerChannel >= 0, "Buffer size increment must be non-negative")
  require(highWaterMarkPercentage > 0.0 && highWaterMarkPercentage <= 1.0, "High watermark must be a strictly positive percentage value")
  require(lowWaterMarkPercentage > 0.0 && lowWaterMarkPercentage <= 1.0, "Low watermark must be a strictly positive percentage value")
  require(lowWaterMarkPercentage < highWaterMarkPercentage, "Low watermark must be strictly below the high watermark")

  val highWatermarkPerc: Float = highWaterMarkPercentage
  val lowWatermarkPerc: Float = lowWaterMarkPercentage
  val initialMaxBufferSize = initialBufferSize
  val bufferSizeIncrement: Int = bufferSizeIncrementPerChannel

  var backpressureMode = false

  var maxBufferSize: Int = 2
  var recordMaxBufferSize = 2
  var lowWatermark: Int = 1
  var highWatermark: Int = 2
  val registeredItems = mutable.Map[ID, C]()

  def resizeBufferAndMarks(): Unit = {
    maxBufferSize = initialBufferSize + bufferSizeIncrement * registeredItems.size
    lowWatermark = scala.math.min(maxBufferSize - 1, scala.math.max(1, (lowWatermarkPerc * maxBufferSize).toInt))
    highWatermark = scala.math.min(maxBufferSize, scala.math.max(lowWatermark + 1, (highWatermarkPerc * maxBufferSize).toInt))
    recordMaxBufferSize = scala.math.max(maxBufferSize, recordMaxBufferSize)
    checkWatermark()
  }

  override val regContext: RegistryContext[C, Unit] = new BasicRegistryContext[C,Unit](registeredItems, mutable.Map[ID, Promise[(C,Unit)]](), mutable.Map[ID, Promise[(C,Unit)]](),
    SimpleReg.expandListeners(Reg.cbBuilder[C].WithBoth((id: ID, c: C) => resizeBufferAndMarks(), (id: ID, c: C) => resizeBufferAndMarks(), Some(Reg.defaultEC)).build()),
    context.dispatcher)

  val queue = mutable.Queue[T]()
  var queueUpdated: B = false
  var terminal: B = false
  var authorization: B = preAuthorized

  override def preStart() = {
    super.preStart()
    resizeBufferAndMarks()
    (flowActorsRegistry, flowActorId) match {
      case (Some(m: AR), Some(id: ID)) => m ++ (Reg.Add[A, B], id, self, true)
      case (_, _) => {}
    }

  }

  override def postStop() = {
    (flowActorsRegistry, flowActorId) match {
      case (Some(m: AR), Some(id: ID)) => m -- (Reg.removeIfMatches[A, B](Some(self)), id, false)
      case (_, _) => {}
    }
    super.postStop()
  }

  def checkWatermark(): Unit = {
    if (!backpressureMode && queue.size > highWatermark) {
      backpressureMode = true
      for ((before, after) <- registeredItems.values) {
        before()
      }
    }
    if (backpressureMode && queue.size < lowWatermark) {
      backpressureMode = false
      for ((before, after) <- registeredItems.values) {
        after()
      }
    }
  }


  override def receive = {
    // receive new msg, add them to the queue, and quickly
    // exit.
    case data: T => {
      // remove the oldest one from the queue and add a new one
      if (!terminal) {


        if (queue.size >= recordMaxBufferSize) {
          log.debug(s"Buffer overflow: dropping new value = $data")
          checkWatermark()
        } else {
          queue += data
          checkWatermark()
          if (!queueUpdated) {
            queueUpdated = true
            self ! QueueUpdated
          }
        }
      }
    }
    // we receive this message if there are new items in the
    // queue. If we have a demand for messages send the requested
    // demand.
    case QueueUpdated => {
      deliver()
    }

    // allow to start publishing
    case Authorized => {
      authorization = true
      deliver()
    }

    // the connected subscriber request n messages, we don't need
    // to explicitly check the amount, we use totalDemand propery for this
    case Request(amount) =>
      deliver()

    // subscriber stops, so we stop ourselves.
    case Cancel => {}
      context.stop(self)

    case Abort(exc) =>
      onErrorThenStop(exc)

    // terminate gracefully
    case Terminal => {
      terminal = true
      deliver()
    }

    case x: Any => super.receive(x)
  }

  /**
   * Deliver the message to the subscriber. Note
   * that even if we have a slow consumer, we won't notice that immediately. First the
   * buffers will fill up before we get feedback.
   */
  @tailrec final def deliver(): Unit = {
    if (terminal && (queue.size == 0 || (!authorization && stopOnTerminalIfNotAuthorized))) {
      // terminate the stream based on external signal
      onCompleteThenStop()
    }

    if (queue.size == 0 && totalDemand != 0) {
      // we can response to queueupdated msgs again, since
      // we can't do anything until our queue contains stuff again.
      queueUpdated = false
    } else if (authorization && totalDemand > 0 && queue.size > 0) {
      val m = queue.dequeue()
      checkWatermark()

      if (queue.size <= highWatermark) {
        recordMaxBufferSize = maxBufferSize
      }

      onNext(m)
      deliver()
    }
  }

}


