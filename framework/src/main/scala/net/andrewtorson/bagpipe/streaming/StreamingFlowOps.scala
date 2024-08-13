/*
 *
 * Author: Andrew Torson
 * Date: Aug 26, 2016
 */

package net.andrewtorson.bagpipe.streaming




import scala.collection.mutable
import scala.concurrent.{Future, Promise}
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

import akka.{Done, NotUsed}
import akka.actor.{ActorRef, ActorRefFactory, Props}
import akka.stream._
import akka.stream.scaladsl._
import net.andrewtorson.bagpipe.utils.{ID, _}


/**
 * Created by Andrew Torson on 8/26/16.
 */
object StreamingFlowOps {

  def actorsReg(implicit system: ActorRefFactory): AR = {
    val callbacks = Reg.cbBuilder[A, B] WithRemove{(id: ID, actor: A, flag: B) => {if (flag) actor ! Terminal}} build()
    new ActorBasedRegistryFacade[A, B](system.actorOf(Props(new RegistryActor[A, B](callbacks))))
  }


  /**
    * Basic streaming flow that produces a new uncontrolled publisher Actor
    * the publisher Actor can be induced to stream by messaging data into it
    * input stream will complete once the complPred is true
    * custom handling can be done based on the result
    * @tparam O
    * @return
    */
  def getBasicUncontrolledStreamingFlow[I: ClassTag, O: ClassTag, U: ClassTag]
  (zero: U, fold: (U,I) => U, continuationPred: I => Boolean)(implicit mat: Materializer): Flow[I, O, (Future[U], ActorRef)] = {

    val flow =  Flow[I].takeWhile(continuationPred, true)

    val sink: Sink[I, Future[U]] = flow.fold[U](zero)(fold).toMat(Sink.headOption)(Keep.right).mapMaterializedValue[Future[U]] {x: Future[Option[U]] =>
      x.map[U]{y: Option[U] => y match {
        case Some(u) => u
        case None => zero
      }}(mat.executionContext)}

    Flow.fromSinkAndSourceMat(sink, Source.actorPublisher[O](StreamingPublisherActor.props[O]()))(Keep.both)
  }

  /**
    * Streams a source to a controlled publisher actor without side effects
    * Actor state is not manipulated
    * @param source
    * @param actorRef
    * @tparam O
    * @return
    */
  def publishFromSource[O](source: Source[O, _], actorRef: ActorRef, sourceID: ID) (implicit mat: Materializer):  RunnableGraph[(UniqueKillSwitch, Future[Done])] = {
    val proxy = new BackpressuredActorPublisherProxy(actorRef, StreamingFlowOps.getActorRouterID(sourceID), scala.concurrent.duration.Duration.Inf)
    val sink = Sink.foreach[O]( proxy.syncSend(_)).mapMaterializedValue[Future[Done]](x => {
      x.onComplete(_ => proxy.terminate())(SameThreadExecutionContext)
      x
    })
    source.viaMat(KillSwitches.single) (Keep.right).toMat(sink)(Keep.both)
  }

  /**
    *
    * Basic streaming flow that induces a given source stream to be streamed out in a controlled fashion
    * Simple control is used: first input element triggers the entire output, the rest of the input is cancelled
    * After the source is depleted, the entire flow will complete
    * @param flowActorRegistry
    * @param flowID
    * @param source
    * @tparam O
    * @return
    */
  def getBasicControlledStreamingFlow[I: ClassTag, O: ClassTag, U: ClassTag](flowActorRegistry: AR,
                                                                             flowID: ID, source: Source[O, _], sourceNameKey: String, captureInterimSourceHistory: Boolean)(zero: U, fold: (U,I) => U)(implicit mat: Materializer): Flow[I, O, (Future[U], ActorRef)] = {

    val flowActorId = getFlowActorID(flowID, true)

    var switch: Option[UniqueKillSwitch] = None

    def publishOnce(source: Source[O, _]) (a: ActorRef): Unit = {
      val (k: UniqueKillSwitch, f: Future[Done]) =  publishFromSource(source,a, ID(ID(ID.FlowCategory, sourceNameKey), flowActorId)).run()
      f.onComplete {case _ => flowActorRegistry -- (Reg.Remove[A,B], StreamingFlowOps.getFlowActorID(flowID, true), true)} (mat.executionContext)
      switch.synchronized{
        switch = Some(k)
      }
    }

    if (captureInterimSourceHistory) flowActorRegistry >>(Reg.Call[A,B](publishOnce(source)), flowActorId)


    val lastSink: Sink[I, Future[U]] = Flow[I].take(1).fold[U](zero)((x,y) => {
      if (captureInterimSourceHistory) {
        flowActorRegistry >> (Reg.Call[A,B](a => a ! Authorized), flowActorId)
      } else {
        flowActorRegistry >> (Reg.Call[A, B](a => {
          publishOnce(source)(a)
          a ! Authorized}), flowActorId)
      }
      fold(x,y)}).toMat(Sink.headOption)(Keep.right).mapMaterializedValue[Future[U]]{x: Future[Option[U]] =>
      x.map[U]{y: Option[U] => y match {
        case Some(u) => u
        case None => zero
      }}(mat.executionContext)}

    val actorSource = Source.actorPublisher[O](StreamingPublisherActor.props[O](Some(flowActorRegistry), Some(flowActorId), false, false)).
      watchTermination()(Keep.both).mapMaterializedValue[ActorRef]{x: (ActorRef, Future[Done]) => {
      x._2.onComplete(_ => switch.synchronized{
        switch match {
          case Some(k) => k.shutdown()
          case _ => {}
        }
      })(mat.executionContext)
      x._1
    }}

    Flow.fromSinkAndSourceMat(lastSink, actorSource)(Keep.both)
  }

  def getFlowActorID(flowID: ID, isOutgoing: Boolean) =  ID(if(isOutgoing) fopaID else fipaID, flowID)

  val fipaID = ID(ID.ActorCategory, "IP")

  val fopaID = ID(ID.ActorCategory, "OP")

  val actorRouterCategoryID = ID(ID.UtilityCategory, "PAR")

  val eventBusSubscriberCategoryID = ID(ID.UtilityCategory, "EBS")

  def getSubFlowID(parentFlowID: ID, entityID: ID)  =  ID(parentFlowID, entityID)

  def getActorRouterID(actorID: ID) = ID(actorRouterCategoryID, actorID)

  def getEventBusSubscriberID(flowID: ID) = ID(eventBusSubscriberCategoryID, getFlowActorID(flowID,false))



  /**
    * Advanced streaming flow that may either switch or add new output streams depending on input message commands
    * WATCHOUT!!: there must be only one running/materialized instance per flowID at a time.
    * Though, this function can be called multiple times per user ID and the resulting flow can be materialized many times
    * This requirement is an consequence of the actor registration and the code could be refactored to eliminate it - but it
    * is meant to be a feature and serves as a fail-fast to ensure
    * that callers know what they do and comply with the general contact that each flow materialization
    * must register with a unique flowID in the registry
    * @param flowActorRegistry
    * @param flowID
    * @param commander
    * @param mat
    * @tparam I
    * @tparam O
    * @tparam U
    * @return
    */

  def getAdaptiveControlledStreamingFlow[I, O, U](flowActorRegistry: AR, flowID: ID,  commander: FlowCommander[I,O, U])(implicit mat: Materializer, it: ClassTag[I], ot: ClassTag[O], ut: ClassTag[U]): Flow[I, O, (Future[U], ActorRef)] = {

    type S = (U,mutable.Queue[UniqueKillSwitch])
    type T = Option[I]

    class InnerWorkflowInstanceState{

      var context: DelayedActorExecution[S,T] = null
      var commander: FlowCommander[I,O,U] = null
      var finalStatePromise: Promise[U] = null
      var actorTerminationCallbackSent: Boolean = false

    }

    val innerWorkflowInstanceState = new InnerWorkflowInstanceState

    val isSwitching = commander.isSwitching
    val closeOutputOnInputTermination = commander.closeOutputOnInputTermination
    val closeInputOnOutputTermination = commander.closeInputOnOutputTermination
    val returnStateOnExceptions = commander.returnStateOnExceptions

    def callback(actorID: ID) (stateOption: Option[S], inputOption: T, actor: ActorRef)
                (implicit mat: Materializer): Option[S] = {
      val internalCommander = innerWorkflowInstanceState.commander
      val finalStatePromise = innerWorkflowInstanceState.finalStatePromise
      val currentState = stateOption.map(_._1).getOrElse(internalCommander.initialState)
      val result = internalCommander((currentState, inputOption))
      val killSwitches = stateOption match{
        case Some(x) => {
          x._2
        }
        case None => {new mutable.Queue[UniqueKillSwitch]()}
      }

      // add termination callback at the overall flow materialization point to stop streaming if actor dies
      // killswitches are enclosed: they are a mutable queue - so it is fine
      if (inputOption.isEmpty && !innerWorkflowInstanceState.actorTerminationCallbackSent) {
        actor ! TerminateCallback(false,{() => killSwitches.synchronized(for (k <-killSwitches) k.shutdown())})
        innerWorkflowInstanceState.actorTerminationCallbackSent = true
      }

      val newState = result._2

      result._3 match {
        case Some(control) => {
          // stream new source
          if (isSwitching && !killSwitches.isEmpty) killSwitches.synchronized(killSwitches.dequeue().shutdown())
          val (k, f) = publishFromSource(control._1, actor, ID(control._2, actorID)) (mat).run ()
          killSwitches.synchronized(killSwitches.enqueue(k))
          f.onComplete {
            case x => {
              control._3 (x)(actor)
            }
          } (mat.executionContext)
        }
        case _ => {}
      }

      if (!result._1){
        //shutdown
        if (closeOutputOnInputTermination){
          for (k <-killSwitches) k.shutdown()
        }
        finalStatePromise.synchronized(if (!finalStatePromise.isCompleted) finalStatePromise.trySuccess(newState))
        Some(newState, killSwitches)
      } else {
        Some(newState, killSwitches)
      }
    }

    val flowActorID = getFlowActorID(flowID, true)

    val sink = Flow[I].viaMat(KillSwitches.single)(Keep.right).map(x => innerWorkflowInstanceState.context.executeItem(Some(x))).toMat(Sink.ignore)(Keep.both)

    val source = Source.actorPublisher[O](StreamingPublisherActor.props[O](Some(flowActorRegistry), Some(flowActorID), true, false)).
      watchTermination()(Keep.both)

    Flow.fromSinkAndSourceMat(sink , source)(Keep.both).mapMaterializedValue(x =>
    {
      innerWorkflowInstanceState.context = new DelayedActorExecution[S,T](callback(flowActorID), true)
      innerWorkflowInstanceState.commander = commander
      innerWorkflowInstanceState.finalStatePromise = Promise[U]
      innerWorkflowInstanceState.actorTerminationCallbackSent = false
      //shutdown input if finalStatePromise is complete
      val f0 = innerWorkflowInstanceState.finalStatePromise.future.andThen{case _ => x._1._1.shutdown()}(SameThreadExecutionContext)
      // complete finalStatePromise if input is shutdown externally
      val f1: Future[U] = x._1._2.andThen{case _  =>
        innerWorkflowInstanceState.context.synchronized {
          innerWorkflowInstanceState.commander = FlowCommander.getStoppingCommander[I, O, U](() =>commander.initialState)
          innerWorkflowInstanceState.context.executeItem(None)
        }
      }(SameThreadExecutionContext).andThen{case _ =>
        if (closeOutputOnInputTermination) flowActorRegistry -- (Reg.Remove[A, B], flowActorID, true)
      }(SameThreadExecutionContext)
        .flatMap(_ => f0)(SameThreadExecutionContext)
        .recoverWith(
          if (returnStateOnExceptions) {case _ => f0} else {PartialFunction.empty}
          // recover on exceptions, if allowed
        )(SameThreadExecutionContext)

      // output termination hooks: no further streaming allowed
      x._2._2.onComplete(_ => {
        innerWorkflowInstanceState.context.synchronized{if (closeInputOnOutputTermination) {
          innerWorkflowInstanceState.commander
            = FlowCommander.getStoppingCommander[I,O,U](() => commander.initialState)
          innerWorkflowInstanceState.context.executeItem(None)
        } else {
          innerWorkflowInstanceState.commander = FlowCommander.getStubCommander(commander)
        }}
      })(SameThreadExecutionContext)
      // actor materialization hook: set actor and ask commander to initialize streaming (if it wants) before the first input arrives
      innerWorkflowInstanceState.context.executeItem(None)
      innerWorkflowInstanceState.context.setActor(x._2._1)
      (f1, x._2._1)
    })

  }


  //ToDo: this needs to be refactored using 'groubBy' feature (very powerful and better preserving backpressure than the blocking publisher proxy does)
  // Group by uses a static flow, while we use dynamic flow that may be different for different flowIDs (necessary for flows which use our StreamingPublisherActor that consumes ID to register)
  // To do that, we'll need to do a custom implementation similar to how groupBy is implemented under the hood.
  def getRoutingCompositeFlow[I: ClassTag, O: ClassTag, In: ClassTag, Out: ClassTag](flowActorRegistry: AR, routingFlowID: ID, flowCodec: Either[FlowCodec[I,O, In, Out], AsyncFlowCodec[I,O,In,Out]], flowGenerator: FlowGenerator[In, Out])(implicit mat: Materializer): Flow[I, O, (Future[Done], ActorRef)] = {

    type R = DelayedActorExecution[BackpressuredActorPublisherProxy, In]
    type CI = (ID, In)
    type CO = (ID, Out)

    val codecProtocol: BidiFlow[I, CI, CO, O, NotUsed] =  flowCodec match {
      case Left(c) => BidiFlow.fromFunctions[I, CI, CO, O](c.decode(_), c.encode(_))
      case Right(c) => BidiFlow.fromFlows(Flow[I].mapAsync(c.parallelism)(c.decode(_)), Flow[CO].mapAsync(c.parallelism)(c.encode(_)))
    }

    class RoutingFlowCommander extends FlowCommander[CI, CO, ConcurrentMapSimpleRegistry[R]] {

      protected def routerCallback(sfipa: ID)(state: Option[BackpressuredActorPublisherProxy], item: In, routee: ActorRef): Option[BackpressuredActorPublisherProxy] ={
        state match {
          case None => {
            val proxy = new BackpressuredActorPublisherProxy(routee, StreamingFlowOps.getActorRouterID(sfipa), scala.concurrent.duration.Duration.Inf)
            proxy.syncSend(item)
            Some(proxy)
          }
          case Some(proxy) => {
            proxy.syncSend(item)
            state
          }
        }
      }

      protected def getRouter(state: ConcurrentMapSimpleRegistry[R], entityID: ID): (Boolean,R) = {
        state.internalExecute(Reg.GetOrAdd[R], entityID, Some({ () => Some(new DelayedActorExecution[BackpressuredActorPublisherProxy,In](
          routerCallback(StreamingFlowOps.getFlowActorID(StreamingFlowOps.getSubFlowID(routingFlowID, entityID), false)), true))}), None) match {
          case Success(Left(Some(x))) => (false, x)
          case Success(Right(Some(x))) => (true, x)
          case _ =>  throw new IllegalArgumentException(s"Failed to retrieve router for the sub flow ${StreamingFlowOps.getSubFlowID(routingFlowID,entityID)}")
        }
      }

      protected def onSubFlowComplete(state: ConcurrentMapSimpleRegistry[R], entityID: ID)(b: Try[Done])(a: ActorRef): Unit = {
        flowActorRegistry -- (Reg.Remove[A,B], StreamingFlowOps.getFlowActorID(StreamingFlowOps.getSubFlowID(routingFlowID, entityID), false), true)
        state -- (Reg.Remove[R], entityID)
        b match {
          // the unerlying flow generator should decorate the flow with flow-specific handler and decide what to do about it
          // if it signaled exception - it indicates to the routing flow to fold with failure
          case Failure(exc) => a ! Abort(new IllegalStateException(s"Sub-flow ${StreamingFlowOps.getSubFlowID(routingFlowID,entityID)} terminated with failure", exc))
          case _ => {}
        }
      }

      override def initialState = new ConcurrentMapSimpleRegistry[R](Reg.cbBuilder[R].build())

      override def apply(in: (ConcurrentMapSimpleRegistry[R], Option[CI]))= {
        val newState = in._1
        in._2 match {
          case Some((entityID: ID, z: In)) => {
            val x = getRouter(newState, entityID)
            val router = x._2
            x._1 match {
              case false => {
                //new entityID - launch flow
                val subFlowID = StreamingFlowOps.getSubFlowID(routingFlowID, entityID)
                flowGenerator[In](subFlowID)(ClassTag[In](z.getClass)) match {
                  case Success(subFlow) => {
                    val sfipa = StreamingFlowOps.getFlowActorID(subFlowID, false)
                    val source = Source.actorPublisher[In](StreamingPublisherActor.props[In](
                      Some(flowActorRegistry), Some(sfipa), true, false)).via(subFlow).map((entityID,_)).mapMaterializedValue(actor => {
                      router.setActor(actor)
                      actor
                    })
                    router.executeItem(z)
                    (true, newState, Some(source, subFlowID, onSubFlowComplete(newState, entityID)))
                  }
                  case Failure(exc) => {
                    newState -- (Reg.Remove[R], entityID)
                    (true, newState, None)
                  }
                }
              }

              case true => {
                // exiting entityID - just route
                router.executeItem(z)
                (true, newState, None)
              }
            }}

          case _ => (true, newState, None)
        }
      }
    }

    codecProtocol.withAttributes(ActorAttributes.supervisionStrategy(Supervision.resumingDecider))
      .joinMat(StreamingFlowOps.getAdaptiveControlledStreamingFlow[CI, CO, ConcurrentMapSimpleRegistry[R]](flowActorRegistry, routingFlowID, new RoutingFlowCommander))(Keep.right)
      .mapMaterializedValue(x => {
        (x._1.map(_ => Done)(SameThreadExecutionContext), x._2)
      })

  }

  class DelayedActorExecution[S, T](executionCallback: (Option[S], T, ActorRef) => Option[S],
                                    accumulateHistory: Boolean)(implicit st: ClassTag[S], tt: ClassTag[T]) {

    protected var state: Option[S] = None
    protected val history = mutable.Queue[T]()
    protected var internalActor: Option[ActorRef] = None


    def setActor(actor: ActorRef): Boolean = {
      this.synchronized({
        internalActor match {
          case None => {
            internalActor = Some(actor)
            for (item <- history) {
              state = executionCallback(state, item, actor)
            }
            val result = history.isEmpty
            history.clear()
            result
          }
          case _ => {
            throw new IllegalStateException("Actor has already been set in this context")
          }
        }
      })
    }

    def executeItem(item: T): Boolean = {

      this.synchronized({

        internalActor match {
          case Some(a) => {
            state = executionCallback(state, item, a)
            true
          }
          case None => {
            if (!accumulateHistory && !history.isEmpty) history.dequeue()
            history.enqueue(item)
            false
          }
        }
      })

    }

  }

}


trait FlowCommander[I,O,U] {

  type Action = (Source[O,_], ID, scala.util.Try[Done] => ActorRef => Unit)

  /**
    * Indicates if output streaming source must be terminated before streaming a new one
    * Note that the new one may be empty -> this way output can be shutdown temporarily in a controllable, state-dependent manner
    */
  val isSwitching: Boolean = false

  val closeOutputOnInputTermination: Boolean = true

  val closeInputOnOutputTermination: Boolean = true

  val returnStateOnExceptions: Boolean = false

  /**
    * Initial state of workflow at the moment of materialization, before any commander apply() invokations
    */
  def initialState: U
  /**
    *
    * @param in combination of current worflow state and new input (if None -> indicates initial state and invokation at the moment of flow materialization)
    * @return combinatiion of continuation signal (true = continue flow, false = stop flow), new state and option of streaming new data into the output
    */
  def apply(in: (U, Option[I])): (Boolean, U,Option[Action])

}

object FlowCommander {

  def getStubCommander[I, O, U](commander: FlowCommander[I,O,U]) = new FlowCommander[I,O,U] {

    override def initialState: U = commander.initialState

    override def apply(in: (U, Option[I]))= {
      val b = commander(in)._1
      (b, in._1, None)
    }
  }

  def getStoppingCommander[I, O, U](state: () =>U) = new FlowCommander[I,O,U] {

    override def initialState = state()

    override def apply(in: (U, Option[I])) = {
      (false, in._1, None)
    }
  }

  def getFailingCommander[I, O,U](exc: Throwable, state:() =>U) = new FlowCommander[I,O,U] {

    override def initialState = state()

    override def apply(in: (U, Option[I])) = {
      throw exc
    }
  }

}

trait FlowGenerator[I,O] {

  def apply[In<:I](flowID: ID)(implicit ct: ClassTag[In]) : Try[Flow[In, _<:O, _]]

}

trait FlowCodec[I, O, In, Out] {


  def encode[T<:Out](value: (ID, T)): O

  def decode(value: I): (ID, _<:In)

}

trait AsyncFlowCodec[I, O, In, Out] {

  val parallelism: Int

  def encode[T<:Out](value: (ID, T)): Future[O]

  def decode(value: I): Future[(ID, _<:In)]

}

object FlowCodec {

  def getIdentityCodec[I<:BaseEntity[I], O] (extractor: (I) => ID) = new FlowCodec[I,O,I,O] {

    override def encode[T<:O](value: (ID, T)) = value._2

    override def decode(value: I) = (extractor(value), value)
  }
}





