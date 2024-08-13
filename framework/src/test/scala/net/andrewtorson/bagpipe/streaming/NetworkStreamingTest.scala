/*
 *
 * Author: Andrew Torson
 * Date: Oct 10, 2016
 */

package net.andrewtorson.bagpipe.streaming





import java.net.InetSocketAddress

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}
import akka.{Done, NotUsed, stream}
import akka.actor.{ActorRef, ActorSystem, ExtendedActorSystem}
import akka.serialization.JavaSerializer
import akka.stream.{ActorMaterializerSettings, _}
import akka.stream.scaladsl.{Flow, GraphDSL, Merge, Source}
import akka.util.ByteString
import net.andrewtorson.bagpipe.networking.NetworkingModule
import net.andrewtorson.bagpipe.utils.{TestConfigurationModule, _}
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.{BeforeAndAfterEach, FunSuite}

/**
 * Created by Andrew Torson on 8/30/16.
 */

sealed trait NetworkStreamingTest_Stage {

}

case object NetworkStreamingTest_Pickup extends NetworkStreamingTest_Stage
case object NetworkStreamingTest_Dropoff extends NetworkStreamingTest_Stage



sealed trait NetworkStreamingTest_JobState {

}

case object NetworkStreamingTest_Pending extends NetworkStreamingTest_JobState
case object NetworkStreamingTest_Started extends NetworkStreamingTest_JobState
case object NetworkStreamingTest_InProgress extends NetworkStreamingTest_JobState
case object NetworkStreamingTest_Finished extends NetworkStreamingTest_JobState

sealed trait NetworkStreamingTest_JobAction {

}

case object NetworkStreamingTest_Dispatch extends NetworkStreamingTest_JobAction
case object NetworkStreamingTest_Track extends NetworkStreamingTest_JobAction
case object NetworkStreamingTest_Complete extends NetworkStreamingTest_JobAction



class NetworkStreamingTest extends FunSuite with BeforeAndAfterEach with LazyLogging {


  trait TestActorModuleImpl extends ActorModule {
    this: ConfigurationModule =>
    val system = ActorSystem("akka-stream-tcp-test", config)
  }

  trait TestTcp[In<:AnyRef,Out<:AnyRef] extends TcpNetworkingModule {

    override type I = In
    override type O = Out

    private val javaSerializer = new JavaSerializer(dependency.system.asInstanceOf[ExtendedActorSystem])

    override def deserialize (bs: ByteString)  = {
      javaSerializer.fromBinary(bs.toArray[Byte]).asInstanceOf[I]
    }

    override def serialize(v: O) = {
      ByteString.fromArray(javaSerializer.toBinary(v))
    }
  }

  class TestTcpServer[In<:AnyRef: ClassTag, Out<:AnyRef: ClassTag](override protected val dependency: ConfigurationModule with ActorModule)
    (flowGen: ID => Flow[In, Out, Future[Done]])(override implicit val itag: ClassTag[In], override implicit val otag: ClassTag[Out]) extends TestTcp[In,Out] with TcpServerModule {


    override def flow(connectionID: ID) = flowGen(connectionID)

  }

  class TestTcpClient[In<:AnyRef: ClassTag, Out<:AnyRef: ClassTag](override protected val dependency: ConfigurationModule with ActorModule)
    (flowGen: ID => Flow[In, Out, Future[Done]])(override implicit val itag: ClassTag[In], override implicit val otag: ClassTag[Out]) extends TestTcp[In,Out] with TcpClientModule {

    override def flow(connectionID: ID) = flowGen(connectionID)

  }

  val timeout = Duration(100, SECONDS)

  var server: TestTcpServer[_,_] = null
  var client: TestTcpClient[_,_] = null
  var testFailure: Try[Done] = Success(Done)


  def awaitCompletion(compl: Future[Done]): Unit = {
    server.start
    logger.debug("Server bound")
    client.start
    logger.debug("Client connected")
    Await.ready(compl, timeout)
    logger.debug("Test finished")
    testFailure match {
      case Success(Done) => {}
      case Failure(exc)  => fail("Failed test due to asserts exception", exc)
    }
  }

  def getPromises()(implicit ec: ExecutionContext): (Promise[Done], Promise[Done], Future[Done]) = {
    val sp = Promise[Done]
    val cp = Promise[Done]
    val f = for {sr <- sp.future
                 cr <- cp.future} yield Done
    (sp, cp, f)
  }

  def decorate[I, O, U](flow: ID => Flow[I, O, (Future[U], ActorRef)], flowHandler: (ID, Future[U], ActorRef)  => Unit)(id: ID)(implicit ec: ExecutionContext) = {
    flow(id).mapMaterializedValue{x: (Future[U], ActorRef) => {
     flowHandler(id, x._1, x._2)
     x._1.map(_ => Done)
    }}
  }

  override def afterEach(): Unit = {
    if (client != null) {
      client.stop
      logger.debug("Client disconnected")
    }
    if (server != null) {
      server.stop
      logger.debug("Server unbound")
    }
    testFailure = Success(Done)
  }

  test("Simple uncorrelated duplex streaming flow") {

    val core = new TestConfigurationModule with TestActorModuleImpl

    implicit val system: ActorSystem = core.system

    implicit val materializer = ActorMaterializer(ActorMaterializerSettings(system).withInputBuffer(
      initialSize = 1,
      maxSize = 1))

    implicit val ec = system.dispatcher

    val (sp, cp, compl) = getPromises()

    def scriptedPrompt(prefix: String)  = Source.unfold[Int, String] (0)
      {x => x match {
        case 0 => Some((1,s"Hi from [$prefix]"))
        case 10 => if (prefix == "S") Some((11, s"[$prefix] has signalled quit")) else Some ((11, s"[$prefix] ready for more data"))
        case 11 => if (prefix == "S") None else Some ((12, s"[$prefix] ready for yet more data"))
        case x => {
          Some ((x+1, s"Data chunk from [$prefix]: $x"))
        }
      }}

    def collectFold(x: Seq[String], y: String): Seq[String] = {
      logger.debug(s"Packet[$y]")
      x :+ y
    }

    def quit(x: String): Boolean = {
      !x.contains("quit")
    }

    def getFlow (flowId: ID): Flow[String, String, (Future[Seq[String]], ActorRef)]  = {
      StreamingFlowOps.getBasicUncontrolledStreamingFlow[String, String, Seq[String]](Seq[String](), collectFold, quit)
    }

    def basicHandler(prefix: String) (flowId: ID, f: Future[Seq[String]], a: ActorRef): Unit = {
      val (k, sc) = StreamingFlowOps.publishFromSource(scriptedPrompt(prefix),a, ID(StreamingFlowOps.getFlowActorID(flowId, true), s"ChatterStream[$prefix]")).run()
      f.onComplete{x => {
        k.shutdown()
        a ! Terminal
        logger.debug(s"$flowId has completed")
        x match {
          case Success(seq: Seq[String]) =>
            try{
              runAsserts(seq, if (prefix == "C") "S" else "C")
            } catch {
              case exc: Exception => testFailure = Failure(exc)
            }

          case Failure(fal) =>testFailure = Failure(fal)
        }
        if (prefix == "S") sp.success(Done) else cp.success(Done)}
      }
    }

    def runAsserts(result: Seq[String], prefix: String): Unit = {
      assert(result.contains(s"Hi from [$prefix]"), s"No greeting message in the results received from [$prefix]")
      if (prefix == "C"){
        //assert(result.size == 11, s"Expected 11 messages in the [$prefix] results but got " + result.size)
        assert(result.contains(s"[$prefix] ready for more data"), s"No continuation message in the results received from [$prefix]")
      } else {
        assert(result(result.size-1).contains("quit"), s"No quit message in the results received from [$prefix]")
        assert(result.size == 11, s"Expected 11 messages in the results received from [$prefix] but got " + result.size)
      }
    }

    server = new TestTcpServer[String,String](core)(decorate(getFlow, basicHandler("S")))
    client = new TestTcpClient[String,String](core)(decorate(getFlow, basicHandler("C")))

    awaitCompletion(compl)
  }


  test("Simple server push streaming flow") {

    val core = new TestConfigurationModule with TestActorModuleImpl

    implicit val system: ActorSystem = core.system

    implicit val materializer = ActorMaterializer(ActorMaterializerSettings(system).withInputBuffer(
      initialSize = 1,
      maxSize = 1))

    implicit val ec = system.dispatcher
    val (sp, cp, compl) = getPromises()

    def stateScript(id: String)  = Source.unfold[Int, String] (0)
      {x => x match {
        case 0 => Some((x+1,s"Entity = $id, State = Created"))
        case 11 => Some((x+1, s"Entity = $id, State = Completed"))
        case 12 => None
        case x => {
          Some ((x+1, s"Entity = $id, State progress update # $x"))
        }
      }}

    def collectFold(x: Seq[String], y: String): Seq[String] = {
      logger.debug(s"Packet[$y]")
      x :+ y
    }

    def getClientFlow (id: ID): Flow[String, String, (Future[Seq[String]], ActorRef)]  = {
      StreamingFlowOps.getBasicUncontrolledStreamingFlow[String, String, Seq[String]](mutable.Seq[String](), collectFold, _ => true)
    }

    def getClientHandler (id: ID, f: Future[Seq[String]], a: ActorRef): Unit = {
      a ! s"Client subscribed for state updates of entity = $id"
      f.onComplete{x => {
        a ! Terminal
        logger.debug(s"$id has completed")
        x match {
          case Success(seq: Seq[String]) =>
            try{
              runClientAsserts(seq)
            } catch {
              case exc: Exception => testFailure = Failure(exc)
            }
            case Failure(fal) =>testFailure = Failure(fal)
        }
        cp.success(Done)
      }}
    }


    def getServerFlow (flowId: ID): Flow[String, String, (Future[Seq[String]], ActorRef)]  = {
      StreamingFlowOps.getBasicControlledStreamingFlow[String, String, Seq[String]](core.streamingActorsRegistry, flowId,
        stateScript(flowId.toString), "EntityLifeCycleStream", true)(Seq[String](), collectFold)
    }

    def getServerHandler (flowId: ID, f: Future[Seq[String]], a: ActorRef): Unit = {
      f.onComplete{x => {
        logger.debug(s"$flowId has completed")
        x match {
          case Success(seq: Seq[String]) =>
            try{
              runServerAsserts(seq)
            } catch {
              case exc: Exception => testFailure = Failure(exc)
            }

          case Failure(fal) =>testFailure = Failure(fal)
        }
        sp.success(Done)
      }}
    }

    def runClientAsserts(result: Seq[String]): Unit = {
      assert(result.size == 12, "Not all of the state updates received by client")
      assert(result(0).contains("State = Created"), "Expected entity creation message received by client")
      assert(result(11).contains("State = Completed"), "Expected entity completion message received by client")
    }

    def runServerAsserts(result: Seq[String]): Unit = {
      assert(result.size == 1, "Expected just the subscription message received by server")
      assert(result(0).contains("Client subscribed"), "Expected the client subscription message received by server")
    }

    server = new TestTcpServer[String,String](core)(decorate(getServerFlow, getServerHandler))
    client = new TestTcpClient[String,String](core)(decorate(getClientFlow, getClientHandler))

    awaitCompletion(compl)
  }

  test("Command/Event workflow") {


    type ServerMessage = (NetworkStreamingTest_Stage, NetworkStreamingTest_JobState, String)

    type ClientMessage = (NetworkStreamingTest_Stage, NetworkStreamingTest_JobAction, String)

    val core = new TestConfigurationModule with TestActorModuleImpl

    implicit val system: ActorSystem = core.system

    implicit val materializer = ActorMaterializer(ActorMaterializerSettings(system).withInputBuffer(
      initialSize = 1,
      maxSize = 1))

    implicit val ec = system.dispatcher

    val (sp, cp, compl) = getPromises()

    def serverScript(stage: NetworkStreamingTest_Stage, action: NetworkStreamingTest_JobAction): Option[Source[ServerMessage, NotUsed]] =  action match {
      case NetworkStreamingTest_Dispatch => Some(Source.unfold[Int, ServerMessage](0) {
        _ match {
          case 0 => Some((1, (stage, NetworkStreamingTest_Started, s"Stage = $stage, State = $NetworkStreamingTest_Started")))
          case 1 => None
        }
      })
      case NetworkStreamingTest_Track => Some(Source.unfold[Int, ServerMessage](1) {
        _ match {
          case x => {
            Some((x + 1, (stage, NetworkStreamingTest_InProgress, s"Stage = $stage, State = $NetworkStreamingTest_InProgress, progress update #[$x]")))
          }
        }
      }.throttle(1, FiniteDuration(500, MILLISECONDS), 0, ThrottleMode.Shaping))
      case NetworkStreamingTest_Complete => Some(Source.unfold[Int, ServerMessage](0) {
        _ match {
          case 0 => Some((1, (stage, NetworkStreamingTest_Finished, s"Stage = $stage, State = $NetworkStreamingTest_Finished")))
          case 1 => None
        }
      })
      case _ => {
        None
      }

    }

    def clientScript(stage: NetworkStreamingTest_Stage, state: NetworkStreamingTest_JobState): Option[Source[ClientMessage, NotUsed]] =   (stage, state) match {

        case (_, NetworkStreamingTest_Pending) => Some(Source.unfold[Int, ClientMessage](0) {
          _ match {
            case 0 => Some((1, (stage, NetworkStreamingTest_Dispatch, s"Stage = $stage, Action = $NetworkStreamingTest_Dispatch")))
            case 1 => None
          }
        })

        case (_, NetworkStreamingTest_Started) => Some(Source.unfold[Int, ClientMessage](0) {
          _ match {
            case 0 => Some((1, (stage, NetworkStreamingTest_Track, s"Stage = $stage, Action = $NetworkStreamingTest_Track")))
            case 1 => None
          }
        })

        case (_, NetworkStreamingTest_InProgress) => Some(Source.unfold[Int, ClientMessage](0) {
          _ match {
            case 0 => Some((1, (stage, NetworkStreamingTest_Complete, s"Stage = $stage, Action = $NetworkStreamingTest_Complete, after 10 sec of progress")))
            case 1 => None
          }
        }.initialDelay(FiniteDuration(5000, MILLISECONDS)))

        case (NetworkStreamingTest_Pickup, NetworkStreamingTest_Finished) => clientScript(NetworkStreamingTest_Dropoff, NetworkStreamingTest_Pending)

        case _ => None

      }

    val wfSourceID = ID.FlowCategory

    def serverCommander = new FlowCommander[ClientMessage, ServerMessage, Seq[ClientMessage]] {

      override def initialState = Seq[ClientMessage]()

      override val isSwitching = true

      override def apply(in: (Seq[ClientMessage],Option[ClientMessage])) = {
        val newState = in._2 match {
          case  Some(x) => {
            logger.debug(s"Packet[$x]")
            in._1 :+ x
          }
          case _ => in._1
        }
        in._2 match {
          case Some((s, a, m)) => (true, newState, serverScript(s,a).map{source => (source, ID(wfSourceID, s"ResponseTo[$m]"), emptyHandler(_))})
          case None => (true, newState, None)
        }
      }
    }



    def clientCommander = new FlowCommander[ServerMessage, ClientMessage, Seq[ServerMessage]] {



      override def initialState = Seq[ServerMessage]()

      override def apply(in: (Seq[ServerMessage],Option[ServerMessage])) = {
        val newState = in._2 match {
          case  Some(x) => {
            logger.debug(s"Packet[$x]")
            in._1 :+ x
          }
          case _ => in._1
        }
        in._2 match {
          case Some((NetworkStreamingTest_Pickup, NetworkStreamingTest_Finished, m)) => (true, newState, clientScript(NetworkStreamingTest_Pickup, NetworkStreamingTest_Finished).map{source => (source, ID(wfSourceID, s"ResponseTo[$m]"),
            loggingHandler("Job drop-off segment dispatch has been dispatched"))})
          case None => (true, newState, clientScript(NetworkStreamingTest_Pickup, NetworkStreamingTest_Pending).map{source => (source,  ID(wfSourceID, s"InitialCommand"),
            loggingHandler("Job pickup segment has been dispatched"))})
          case Some((NetworkStreamingTest_Dropoff, NetworkStreamingTest_Finished, m)) => (false, newState, None)
          case Some((s, NetworkStreamingTest_InProgress, m)) => {
            if (m.contains("#[10]")) (true,  newState, clientScript(s, NetworkStreamingTest_InProgress).map{source => (source,  ID(wfSourceID, s"ResponseTo[$m]"), emptyHandler(_))})
            else (true,  newState, None)
          }
          case Some((s, st, m)) => (true, newState,  clientScript(s, st).map{source => (source,  ID(wfSourceID, s"ResponseTo[$m]"), emptyHandler(_))})
        }
      }
    }

    def emptyHandler(b: Try[Done])(a: ActorRef): Unit = {}

    def loggingHandler(m: String)(b: Try[Done])(a: ActorRef): Unit = {
      logger.debug(m)
    }


    def getClientFlow (id: ID): Flow[ServerMessage , ClientMessage, (Future[Seq[ServerMessage]], ActorRef)]  = {
      StreamingFlowOps.getAdaptiveControlledStreamingFlow[ServerMessage, ClientMessage, Seq[ServerMessage]] (core.streamingActorsRegistry, id, clientCommander)
    }


    def getClientHandler (id: ID, f: Future[Seq[ServerMessage]], a: ActorRef): Unit = {
      f.onComplete{x => {
        logger.debug(s"$id has completed")
        x match {
          case Success(seq: Seq[ServerMessage]) =>
           try{
             runClientAsserts(seq)
           } catch {
             case exc: Exception => testFailure = Failure(exc)
           }

          case Failure(fal) => testFailure = Failure(fal)
        }
        cp.success(Done)
      }}
    }


    def getServerFlow (id: ID): Flow[ClientMessage, ServerMessage, (Future[Seq[ClientMessage]], ActorRef)]  = {
        StreamingFlowOps.getAdaptiveControlledStreamingFlow[ClientMessage, ServerMessage, Seq[ClientMessage]] (core.streamingActorsRegistry,id, serverCommander)
    }

    def getServerHandler (id: ID, f: Future[Seq[ClientMessage]], a: ActorRef): Unit = {
      f.onComplete{x => {
        logger.debug(s"$id has completed")
        x match {
          case Success(seq: Seq[ClientMessage]) =>
            try{
              runServerAsserts(seq)
            } catch {
              case exc: Exception => testFailure = Failure(exc)
            }
          case Failure(fal) => testFailure = Failure(fal)
        }
        sp.success(Done)
      }}
    }

    def runClientAsserts(result: Seq[ServerMessage]): Unit = {
      assert(result.size >= 24, "Not all of the state updates received by client")
      assert(result(0)._1 == NetworkStreamingTest_Pickup && result(0)._2 == NetworkStreamingTest_Started, "Expected the server pickup started job state received by server")
      assert(result(result.size-1)._1 == NetworkStreamingTest_Dropoff && result(result.size-1)._2 == NetworkStreamingTest_Finished, "Expected the server dropoff finished job state received by server")
    }

    def runServerAsserts(result: Seq[ClientMessage]): Unit = {
      assert(result.size == 6, "Expected just the subscription message received by server")
      assert(result(0)._1 == NetworkStreamingTest_Pickup && result(0)._2 == NetworkStreamingTest_Dispatch, "Expected the client pickup dispatch command received by server")
      assert(result(5)._1 == NetworkStreamingTest_Dropoff && result(5)._2 == NetworkStreamingTest_Complete, "Expected the client dropoff complete command received by server")
    }

    server = new TestTcpServer[ClientMessage,ServerMessage](core)(decorate(getServerFlow, getServerHandler))
    client = new TestTcpClient[ServerMessage,ClientMessage](core)(decorate(getClientFlow, getClientHandler))

    awaitCompletion(compl)
  }

  test("Complex multiple streaming workflows") {


    type ServerEntityMessage = (NetworkStreamingTest_Stage, NetworkStreamingTest_JobState, String)

    type ClientEntityMessage = (NetworkStreamingTest_Stage, NetworkStreamingTest_JobAction, String)

    type ServerMessage = (ID, ServerEntityMessage)

    type ClientMessage = (ID, ClientEntityMessage)

    val core = new TestConfigurationModule with TestActorModuleImpl

    implicit val system: ActorSystem = core.system

    implicit val materializer = ActorMaterializer(ActorMaterializerSettings(system).withInputBuffer(
      initialSize = 1,
      maxSize = 1))

    implicit val ec = system.dispatcher

    var lastJob: Long = 200L
    val jobPeriodMillis: Long = 200L
    val jobInProgressUpdatePeriodMillis: Long = 10L
    val jobStageCompletionDelayMillis: Long = 20000L

    val (sp, cp, compl) = getPromises()

    def serverEntityScript(entity: ID, stage: NetworkStreamingTest_Stage, action: NetworkStreamingTest_JobAction): Option[Source[ServerEntityMessage, NotUsed]] =  action match {
      case NetworkStreamingTest_Dispatch => Some(Source.unfold[Int, ServerEntityMessage](0) {
        _ match {
          case 0 => Some((1, (stage, NetworkStreamingTest_Started, s"Entity = $entity, Stage = $stage, State = $NetworkStreamingTest_Started")))
          case 1 => None
        }
      })
      case NetworkStreamingTest_Track => Some(Source.unfold[Int, ServerEntityMessage](1) {
        _ match {
          case x => {
            Some((x + 1, (stage, NetworkStreamingTest_InProgress, s"Entity = $entity, Stage = $stage, State = $NetworkStreamingTest_InProgress, progress update #[$x]")))
          }
        }
      }.throttle(1, FiniteDuration(jobInProgressUpdatePeriodMillis, MILLISECONDS), 0, ThrottleMode.Shaping))
      case NetworkStreamingTest_Complete => Some(Source.unfold[Int, ServerEntityMessage](0) {
        _ match {
          case 0 => Some((1, (stage, NetworkStreamingTest_Finished, s"Entity = $entity, Stage = $stage, State = $NetworkStreamingTest_Finished")))
          case 1 => None
        }
      })
      case _ => {
        None
      }

    }

    def clientEntityScript(entity: ID, stage: NetworkStreamingTest_Stage, state: NetworkStreamingTest_JobState): Option[Source[ClientEntityMessage, NotUsed]] =   (stage, state) match {

      case (_, NetworkStreamingTest_Pending) => Some(Source.unfold[Int, ClientEntityMessage](0) {
        _ match {
          case 0 => Some((1, (stage, NetworkStreamingTest_Dispatch, s"Entity = $entity, Stage = $stage, Action = $NetworkStreamingTest_Dispatch")))
          case 1 => None
        }
      })

      case (_, NetworkStreamingTest_Started) => Some(Source.unfold[Int, ClientEntityMessage](0) {
        _ match {
          case 0 => Some((1, (stage, NetworkStreamingTest_Track, s"Entity = $entity, Stage = $stage, Action = $NetworkStreamingTest_Track")))
          case 1 => None
        }
      })

      case (_, NetworkStreamingTest_InProgress) => Some(Source.unfold[Int, ClientEntityMessage](0) {
        _ match {
          case 0 => Some((1, (stage, NetworkStreamingTest_Complete, s"Entity = $entity, Stage = $stage, Action = $NetworkStreamingTest_Complete, after 10 sec of progress")))
          case 1 => None
        }
      }.initialDelay(FiniteDuration(jobStageCompletionDelayMillis, MILLISECONDS)))

      case (NetworkStreamingTest_Pickup, NetworkStreamingTest_Finished) => clientEntityScript(entity, NetworkStreamingTest_Dropoff, NetworkStreamingTest_Pending)

      case _ => None

    }

    val wfSourceId = ID.FlowCategory

    def serverEntityCommander(sourceActorId: ID, entityId: ID) = new FlowCommander[ClientEntityMessage, ServerEntityMessage, Seq[ClientEntityMessage]] {


      override val isSwitching = true

      override def initialState = Seq[ClientEntityMessage]()

      override def apply(in: (Seq[ClientEntityMessage], Option[ClientEntityMessage])) = {
        val newState = in._2 match {
          case  Some(x) => in._1 :+ x
          case _ => in._1
        }
        in._2 match {
          case None => (true, newState, None)
          case Some((NetworkStreamingTest_Dropoff, NetworkStreamingTest_Complete, m)) => (true, newState, serverEntityScript(entityId,NetworkStreamingTest_Dropoff,NetworkStreamingTest_Complete).map{source => (source, ID(wfSourceId, s"ResponseTo[$m]"),
            {b: Try[Done] => {a: ActorRef => {
              terminate(core.streamingActorsRegistry, sourceActorId)
            }}})})
          case Some((s, a, m)) => (true, newState, serverEntityScript(entityId, s,a).map{source => (source,  ID(wfSourceId, s"ResponseTo[$m]"), emptyHandler(_))})
        }
      }
    }

    def clientEntityCommander(entityID: ID) = new FlowCommander[ServerEntityMessage, ClientEntityMessage, Seq[ServerEntityMessage]] {


      override def initialState = Seq[ServerEntityMessage]()

      override def apply(in: (Seq[ServerEntityMessage],Option[ServerEntityMessage])) = {
        val newState = in._2 match {
          case  Some(x) => in._1 :+ x
          case _ => in._1
        }
        in._2 match {

          case None => (true, newState, None)
          case Some((NetworkStreamingTest_Pickup, NetworkStreamingTest_Pending, m)) => (true, newState, clientEntityScript(entityID, NetworkStreamingTest_Pickup, NetworkStreamingTest_Pending).map{source => (source, ID(wfSourceId, s"ResponseTo[$m]"), loggingHandler(s"$entityID pickup segment has been dispatched"))})
          case Some((NetworkStreamingTest_Pickup, NetworkStreamingTest_Finished, m)) => (true, newState, clientEntityScript(entityID, NetworkStreamingTest_Pickup, NetworkStreamingTest_Finished).map{source => (source, ID(wfSourceId, s"ResponseTo[$m]"), loggingHandler(s"$entityID drop-off segment has been dispatched"))})
          case Some((NetworkStreamingTest_Dropoff, NetworkStreamingTest_Finished, m)) => (false, newState, None)
          case Some((s, NetworkStreamingTest_InProgress, m)) => {
            if (m.contains("#[10]")) (true, newState, clientEntityScript(entityID, s, NetworkStreamingTest_InProgress).map{source => {
              (source, ID(wfSourceId, s"ResponseTo[$m]"), emptyHandler(_))
            }})
            else (true, newState, None)
          }
          case Some((s, st, m)) => (true, newState, clientEntityScript(entityID,s, st).map{source => (source, ID(wfSourceId, s"ResponseTo[$m]"), emptyHandler(_))})
        }
      }
    }

    def terminate(flowActorRegistry: AR, sourceActorId: ID): Unit ={
      flowActorRegistry -- (Reg.Remove[A,B], sourceActorId, true)
    }

    def emptyHandler(b: Try[Done])(a: ActorRef): Unit = {}


    def loggingHandler(m: String)(b: Try[Done])(a: ActorRef): Unit = {
      logger.debug(m)
    }

    def loggingAccumulatorFold[T:ClassTag](x: Seq[T], y: T): Seq[T] = {
 /*     y match {
        case (_,_,v) =>  logger.debug(s"Packet[$v]")
        case _ => None
      }*/
      x :+ y
    }

    def serverFlowGenerator(mainFlowID: ID) = new FlowGenerator[ClientEntityMessage, ServerEntityMessage]() {

      override def apply[In<:ClientEntityMessage](flowID: ID)(implicit ct: ClassTag[In])= {

        val entityID = flowID.findAncestor(Seq[ID](ID.EntityCategory), true).get

        Success(StreamingFlowOps.getAdaptiveControlledStreamingFlow[ClientEntityMessage, ServerEntityMessage, Seq[ClientEntityMessage]](
          core.streamingActorsRegistry, flowID, serverEntityCommander(StreamingFlowOps.getFlowActorID(flowID,false), entityID)).mapMaterializedValue[(Future[Seq[ClientEntityMessage]], ActorRef)](x => {
          x._1.onComplete(y => {
            try {
              runServerAsserts(entityID, y)
            } catch {
              case exc: Exception => {
                testFailure = Failure(exc)
                core.streamingActorsRegistry -- (Reg.Remove[A,B], StreamingFlowOps.getFlowActorID(mainFlowID, true),true)
                lastJob = 0
              }
            }
            val status = if (y.isSuccess) "Success" else "Failure"
            logger.debug(s"Server flow completed for $entityID with $status")
          })
          x
        }))
      }
    }

    def clientFlowGenerator(mainFlowID: ID) = new FlowGenerator[ServerEntityMessage,ClientEntityMessage] () {

      val lastJobId = ID(ID(ID.EntityCategory, "JOB"), lastJob)

      override def apply[In<:ServerEntityMessage](flowID: ID)(implicit ct: ClassTag[In]) = {

        val entityID = flowID.findAncestor(Seq[ID](ID.EntityCategory), true).get

        Success(StreamingFlowOps.getAdaptiveControlledStreamingFlow[ServerEntityMessage, ClientEntityMessage, Seq[ServerEntityMessage]](
          core.streamingActorsRegistry, flowID, clientEntityCommander(entityID)).mapMaterializedValue[(Future[Seq[ServerEntityMessage]], ActorRef)](x => {
          x._1.onComplete(y => {
            try {
              runClientAsserts(entityID, y)
            } catch {
              case exc: Exception => {
                testFailure = Failure(exc)
                core.streamingActorsRegistry -- (Reg.Remove[A, B], StreamingFlowOps.getFlowActorID(mainFlowID, true), true)
                lastJob = 0
              }
            }
            if (entityID == lastJobId) {
              core.streamingActorsRegistry -- (Reg.Remove[A, B], StreamingFlowOps.getFlowActorID(mainFlowID, true), true)
            }
            val status = if (y.isSuccess) "Success" else "Failure"
            logger.debug(s"Client flow completed for $entityID with $status")
          })
          x
        }))
      }
    }

    def getIdentityCodec[I, O] = new FlowCodec[(ID,I),(ID,O),I,O] {
      override def encode[T <: O](value: (ID, T)) = value
      override def decode(value: (ID, I)) = value
    }


    def getMainServerFlow(mainFlowID: ID): Flow[ClientMessage, ServerMessage, (Future[Done], ActorRef)] = {
       StreamingFlowOps.getRoutingCompositeFlow[ClientMessage, ServerMessage, ClientEntityMessage, ServerEntityMessage](
         core.streamingActorsRegistry, mainFlowID, Left(getIdentityCodec[ClientEntityMessage, ServerEntityMessage]), serverFlowGenerator(mainFlowID))
    }

    def getMainClientFlow(mainFlowID: ID): Flow[ServerMessage, ClientMessage,  (Future[Done], ActorRef)] = {
      val g = StreamingFlowOps.getRoutingCompositeFlow[ServerMessage, ClientMessage, ServerEntityMessage, ClientEntityMessage](
        core.streamingActorsRegistry, mainFlowID, Left(getIdentityCodec[ServerEntityMessage, ClientEntityMessage]), clientFlowGenerator(mainFlowID))

      Flow.fromGraph(GraphDSL.create(g){ implicit b: GraphDSL.Builder[(Future[Done], ActorRef)] ⇒ f: FlowShape[ServerMessage, ClientMessage] ⇒

        import GraphDSL.Implicits._

        val pendingSource = b.add(Source.unfold[Long, ServerMessage](1) {
          _ match {
            case x => {
              if (x <= lastJob) Some((x + 1, (ID(ID(ID.EntityCategory, "JOB"), x), (NetworkStreamingTest_Pickup, NetworkStreamingTest_Pending, s"Created job #$x, Stage = $NetworkStreamingTest_Pickup, State = $NetworkStreamingTest_Pending")))) else None
            }
          }
        }.throttle(1, FiniteDuration(jobPeriodMillis, MILLISECONDS), 0, ThrottleMode.Shaping))


        val merge = b.add(Merge[ServerMessage](2))


        pendingSource ~> merge.in(1)
        merge.out ~> f.in


        FlowShape.of[ServerMessage, ClientMessage](merge.in(0), f.out)

      })

    }


    def getMainServerFlowHandler (id: ID, f: Future[Done], a: ActorRef): Unit = {
      f.onComplete{x => {
        logger.debug(s"$id has completed")
        sp.success(Done)
      }}
    }

    def getMainClientFlowHandler (id: ID, f: Future[Done], a: ActorRef): Unit = {
      f.onComplete{x => {
        logger.debug(s"$id has completed")
        cp.success(Done)
      }}
    }

    def runClientAsserts(id: ID, input: Try[Seq[ServerEntityMessage]]): Unit = {
      input match{
        case Success(seq) => internalClientAsserts(seq)
        case Failure(exc) => fail(s"Exceptionally failed client asserts for job #$id",exc)
      }
    }

    def runServerAsserts(id: ID, input: Try[Seq[ClientEntityMessage]]): Unit = {
      input match{
        case Success(seq) => internalServerAsserts(seq)
        case Failure(exc) => fail(s"Exceptionally failed server asserts for job #$id",exc)
      }
    }

    def internalClientAsserts(result: Seq[ServerEntityMessage]): Unit = {
      assert(result.size >= 25, "Not all of the state updates received by client")
      assert(result(1)._1 == NetworkStreamingTest_Pickup && result(1)._2 == NetworkStreamingTest_Started, "Expected the pickup started job state received by client")
      assert(result(result.size-1)._1 == NetworkStreamingTest_Dropoff && result(result.size-1)._2 == NetworkStreamingTest_Finished, "Expected the dropoff finished job state received by client")
    }

    def internalServerAsserts(result: Seq[ClientEntityMessage]): Unit = {
      assert(result.size == 6, "Not all of the job action commands received by server")
      assert(result(0)._1 == NetworkStreamingTest_Pickup && result(0)._2 == NetworkStreamingTest_Dispatch, "Expected the pickup dispatch command received by server")
      assert(result(5)._1 == NetworkStreamingTest_Dropoff && result(5)._2 == NetworkStreamingTest_Complete, "Expected the dropoff complete command received by server")
    }

    server = new TestTcpServer[ClientMessage,ServerMessage](core)(decorate(getMainServerFlow, getMainServerFlowHandler))
    client = new TestTcpClient[ServerMessage,ClientMessage](core)(decorate(getMainClientFlow, getMainClientFlowHandler))

    awaitCompletion(compl)

  }

}
