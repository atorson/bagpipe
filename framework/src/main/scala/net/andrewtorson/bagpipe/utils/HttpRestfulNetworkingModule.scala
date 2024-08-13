/*
 *
 * Author: Andrew Torson
 * Date: Dec 13, 2016
 */

package net.andrewtorson.bagpipe.utils

import java.net.InetSocketAddress

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}
import akka.{Done, NotUsed}
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.{HostConnectionPool, ServerBinding}
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, HttpResponse, Uri}
import akka.http.scaladsl.server.{Route, RouteConcatenation}
import akka.stream._
import akka.stream.scaladsl.{Flow, GraphDSL, Keep, Merge, MergePreferred, Sink, Source}
import net.andrewtorson.bagpipe.ApplicationService
import net.andrewtorson.bagpipe.networking.{BE, _}
import net.andrewtorson.bagpipe.rest.ProtoJsonSupport

import scala.language.existentials
import akka.actor.ActorRef
import akka.http.scaladsl.client.RequestBuilding
import akka.http.scaladsl.marshalling.{Marshal, _}
import akka.http.scaladsl.settings.ConnectionPoolSettings
import akka.http.scaladsl.unmarshalling.Unmarshal
import net.andrewtorson.bagpipe.entities.AuditDef
import de.heikoseeberger.akkasse.MediaTypes._
import de.heikoseeberger.akkasse.ServerSentEvent
import net.andrewtorson.bagpipe.streaming.{AsyncFlowCodec, FlowCommander, StreamingFlowOps}



/**
 * Created by Andrew Torson on 12/13/16.
 */

trait HttpRestfulServerModule extends ActorBasedNetworkingModule with ApplicationService[HttpRestfulServerModule] {

  override val protocol =  IO.REST_PROTOCOL

  override type I = HttpRequest
  override type O = HttpResponse
  override type Repr = Any

  override implicit val itag = ClassTag[HttpRequest](classOf[HttpRequest])
  override implicit val otag = ClassTag[HttpResponse](classOf[HttpResponse])

  import scala.concurrent.duration._

  override val tag =  ClassTag[HttpRestfulServerModule](classOf[HttpRestfulServerModule])

  override val abnormalTerminationCode = 4

  protected var innerBinding: Option[ServerBinding] = None

  protected val localHost = dependency.config.getString("networking.http.localHost")
  protected val localPort = dependency.config.getInt("networking.http.localPort")


  override def isRunning = innerBinding.isDefined

  override def start = if (!isRunning) this.synchronized{
    Try{
      Await.result(
        Http().bind(localHost, localPort).toMat(Sink.foreach(x => {
          val connectionID = NetworkingModule.getConnectionFlowID(x.remoteAddress, x.localAddress, true, protocol)
          x.flow.join(flow(connectionID)).run()})
        )(Keep.left).run()
        , 10.seconds)
    } match {
      case Success(x) => {
        innerBinding = Some(x)
        true
      }
      case _ => false
    }
  } else {false}


  override def stop: Boolean = if (isRunning) this.synchronized{
    Try{
      Await.result(innerBinding.get.unbind(), 10.seconds)
    } match {
      case Success(_) => {
        innerBinding = None
        true
      }
      case _ => false
    }
  } else {false}

}

trait HttpRestfulClientModule extends ActorBasedNetworkingModule with ApplicationService[HttpRestfulClientModule]{

  import scala.concurrent.duration._

  override val protocol =  IO.REST_PROTOCOL

  override type I = HttpResponse
  override type O = HttpRequest

  override implicit val itag = ClassTag[HttpResponse](classOf[HttpResponse])
  override implicit val otag = ClassTag[HttpRequest](classOf[HttpRequest])

  override type Repr = Future[Done]

  override val tag =  ClassTag[HttpRestfulClientModule](classOf[HttpRestfulClientModule])

  override val abnormalTerminationCode = 5

  protected var innerBinding: Option[(HostConnectionPool,UniqueKillSwitch)] = None

  protected val remoteHost = dependency.config.getString("networking.http.remoteHost")
  protected val remotePort = dependency.config.getInt("networking.http.remotePort")
  protected val localHost = dependency.config.getString("networking.http.localHost")

  protected def poolSettings: ConnectionPoolSettings

  override def isRunning = innerBinding.isDefined

  override def start = if (!isRunning) this.synchronized{
    Try{
      val remoteAddress = new InetSocketAddress(remoteHost, remotePort)
      val localAddress = new InetSocketAddress(localHost, 0)
      val connectionID = NetworkingModule.getConnectionFlowID(localAddress, remoteAddress, false, protocol)

      val x = Http().cachedHostConnectionPool[NotUsed](remoteHost, remotePort, poolSettings)
        .collect{case (Success(x),_) => x}.viaMat(KillSwitches.single)(Keep.both)
        .joinMat(flow(connectionID).map((_,NotUsed)))(Keep.both).run()
      x._2.onComplete(_ => stop)(actorSystem.dispatcher)
      x._1
    } match {
      case Success(x) => {
        innerBinding = Some(x)
        true
      }
      case _ => false
    }
  } else {false}


  override def stop: Boolean = if (isRunning) this.synchronized{
    Try{
      val x = innerBinding.get
      x._2.shutdown()
      Await.ready(x._1.shutdown(),10.seconds)
    } match {
      case Success(_) => {
        innerBinding = None
        true
      }
      case _ => false
    }
  } else {false}

}

class HttpRestfulClientModuleProto(override protected val dependency: ConfigurationModule with ActorModule) extends HttpRestfulClientModule {


  outer =>

  import akka.http.scaladsl.marshalling._
  //import scala.concurrent.duration._
  import de.heikoseeberger.akkasse.EventStreamUnmarshalling._

  class InnerHttpEntityWiseClientModule extends EntityWiseNetworkingModule{

    override protected val dependency = outer.dependency

    protected lazy val entityTransformers: Map[String, (ID, String => BE)] = EntityDefinition.all().groupBy(_.categoryID.nameKey)
      .mapValues(x => {
        val y = x.head
        (y.categoryID, ProtoJsonSupport.fromJsonStringTransformer(y.companion).asInstanceOf[String => BE])
      })

    override type I = ServerSentEvent
    override type O = HttpRequest


    override val codec = Right(new AsyncFlowCodec[ServerSentEvent, HttpRequest, BE, BE]{

      override val parallelism: Int = 4

      override def encode[E<:BE](value: (ID, E)) =  {

        val v = value._2
        implicit val marshaller = v.definition.rest.entityJsonMarshaller.asInstanceOf[ToEntityMarshaller[E]]
        v.version match {
          case x if (x > 1) =>  Marshal((HttpMethods.PUT, Uri(s"/${v.definition.rest.path}"), Nil, v)).to[HttpRequest]
          case _ => Marshal((HttpMethods.POST, Uri(s"/${v.definition.rest.path}"), Nil, v)).to[HttpRequest]
        }
      }


      override def decode(value: ServerSentEvent) = Future[(ID, BE)]{

        value match {
          case ServerSentEvent(Some(data), Some(category), _, _) => {
            entityTransformers.get(category) match {
              case Some(x) => {
                val r = (x._1, x._2(data))
                r
              }
              case _ => getDecodingFailure(value)
            }
          }
          case _ =>  getDecodingFailure(value)
        }
      }

      private def getDecodingFailure(value: ServerSentEvent): Nothing = throw new IllegalArgumentException(s"Failed to decode server-side event $value")
    })

    protected def getTransformer[E <: BaseEntity[E]](warmUpEntity: BE) (implicit ct: ClassTag[E]) = ProtoJsonSupport.toJsonStringTransformer[E]

    protected def getEntity[E <: BaseEntity[E]](warmUpEntity: BE) (implicit ct: ClassTag[E]): E = warmUpEntity.asInstanceOf[E]

    override protected def transformWarmupEntity(warmUpEntity: BE) = {
      if (warmUpEntity.definition.tag == AuditDef.tag) {
        Future.failed[ServerSentEvent](new IllegalArgumentException(s"Prohibited warm-up entity $warmUpEntity"))
      } else {
        Future{
          ServerSentEvent(Some(getTransformer(warmUpEntity)(warmUpEntity.definition.tag)(getEntity(warmUpEntity)(warmUpEntity.definition.tag))),
            Some(warmUpEntity.definition.categoryID.nameKey), Some(warmUpEntity.namekey))}
      }
    }

    override val protocol = outer.protocol

    override type Repr = outer.Repr

    override implicit val itag = ClassTag[ServerSentEvent](classOf[ServerSentEvent])
    override implicit val otag = ClassTag[HttpRequest](classOf[HttpRequest])

  }

  protected def roundUpToPowerOf2(x: Int) = scala.math.pow(2, scala.math.ceil(scala.math.log(x)/scala.math.log(2))).toInt

  protected val maxConnections = Try{dependency.config.getInt("akka.http.host-connection-pool.max-connections")} match{
    case Success(x) => x
    case _ => 4
  }
  protected val maxOpenRequests = Try{dependency.config.getInt("akka.http.host-connection-pool.max-open-requests")} match{
    case Success(x) => x
    case _ => 32
  }

  override protected def poolSettings = {
    val movingTarget = EntityDefinition.all.size
    ConnectionPoolSettings(actorSystem.settings.config)
      .withMaxConnections(movingTarget + maxConnections)
      .withMaxOpenRequests(roundUpToPowerOf2(movingTarget + maxOpenRequests))
      .withPipeliningLimit(1)
  }


  protected val innerEntityWiseClientModuleProto = new InnerHttpEntityWiseClientModule

  private val sseParentID = ID(ID.UtilityCategory, "SSE")

  protected val transformer = new FlowCommander[Source[ServerSentEvent, Any], ServerSentEvent, NotUsed] {

    override def initialState = NotUsed

    override def apply(in: (NotUsed, Option[Source[ServerSentEvent, Any]])) = {
      in._2 match {
        case Some(source) => {
          (true, NotUsed, Some((source, ID(sseParentID), {x: Try[Done] => {y: ActorRef => {}}})))
        }
        case _ => (true, NotUsed, None)
      }
    }
  }

  protected def getTransformerFlow(flowID: ID) = StreamingFlowOps.getAdaptiveControlledStreamingFlow[Source[ServerSentEvent, Any], ServerSentEvent, NotUsed](
    dependency.streamingActorsRegistry, flowID, transformer)


  protected def isSSE(response: HttpResponse): Boolean = (response.entity.isChunked && response.entity.getContentType().mediaType == `text/event-stream`)

  protected val sseDecodeFlow = {
    Flow[HttpResponse].map(x => isSSE(x) match {
      case true => x
      case _ => {
        x.discardEntityBytes()
        x
      }
    }).filter(isSSE(_)).mapAsync(1)(Unmarshal(_).to[Source[ServerSentEvent, NotUsed]])
  }

  protected def transformWarmupEntity[E<:BaseEntity[E]](definition: EntityDefinition[E]): Future[O] = {
    if (definition.tag == AuditDef.tag) {
      Future.failed(new IllegalArgumentException(s"Prohibited warm-up entity ${definition.tag}"))
    } else {
      Future{
       RequestBuilding.Get(s"/${definition.rest.path}/Stream")
      }
    }
  }

  protected def warmUpFlow(g: Flow[I, O, Future[Done]]) =
    Flow.fromGraph(GraphDSL.create(g){ implicit b: GraphDSL.Builder[Future[Done]] ⇒ f: FlowShape[I, O] ⇒

      import GraphDSL.Implicits._

      val warmUpSource = b.add(EntityWiseNetworkingModule.getWarmupEntitySource[O]{x: BE => transformWarmupEntity(x.definition)})

      val merge = b.add(MergePreferred[O](1))

      f.out ~> merge.in(0)
      warmUpSource ~> merge.preferred

      FlowShape.of[I, O](f.in, merge.out)

    })


  override def flow(connectionID: ID) =
    warmUpFlow(sseDecodeFlow.viaMat(getTransformerFlow(ID(connectionID, "SSETR")).viaMat(
      innerEntityWiseClientModuleProto.flow(ID(connectionID, "SSEFL")))(Keep.right))(Keep.right))


  ApplicationService.register[HttpRestfulClientModule](this)

}

class HttpRestfulServerModuleProto(override protected val dependency: ConfigurationModule with ActorModule) extends HttpRestfulServerModule with RouteConcatenation with CorsSupport{

  lazy val serverRoute = Route.handlerFlow({
    // JSON-over-HTTP service
    corsHandler(EntityDefinition.all().map(_.rest.routes).reduce(_ ~ _))
  })

  override def flow(connectionID: ID) = serverRoute

  ApplicationService.register[HttpRestfulServerModule](this)

}



