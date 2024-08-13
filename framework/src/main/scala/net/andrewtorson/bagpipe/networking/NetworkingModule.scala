/*
 *
 * Author: Andrew Torson
 * Date: Jan 27, 2017
 */

package net.andrewtorson.bagpipe.networking

import java.net.InetSocketAddress

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.reflect.ClassTag
import scala.util.{Success, Try}
import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, FlowShape, Materializer}
import akka.stream.scaladsl.{Flow, GraphDSL, Keep, Merge, MergePreferred, Sink, Source}
import net.andrewtorson.bagpipe.networking.IO.NetworkingProtocol
import net.andrewtorson.bagpipe.utils.{BaseEntity, _}

import scala.language.existentials
import net.andrewtorson.bagpipe.networking._
import net.andrewtorson.bagpipe.streaming.{AsyncFlowCodec, FlowCodec, FlowGenerator, StreamingFlowOps}


/**
 * Created by Andrew Torson on 8/26/16.
 */

trait NetworkingModule {

  val protocol: NetworkingProtocol

  type I
  type O
  type Repr >: Future[Done]

  implicit val itag: ClassTag[I]
  implicit val otag: ClassTag[O]

  def flow(connectionID: ID): Flow[I, O, Repr]

}

trait ActorBasedNetworkingModule extends NetworkingModule{

  protected val dependency: ConfigurationModule with ActorModule

  implicit lazy val actorSystem: ActorSystem = dependency.system
  implicit lazy val materializer = ActorMaterializer()
  implicit lazy val ec = actorSystem.dispatcher

}

trait CompositeFlowNetworkingModule extends ActorBasedNetworkingModule{

  type EntityRepr

  implicit val ertag: ClassTag[EntityRepr]

  val codec: Either[FlowCodec[I,O, EntityRepr, EntityRepr], AsyncFlowCodec[I,O, EntityRepr, EntityRepr]]

  protected def transformWarmupEntity(warmUpEntity: BE): Future[I]

  val flowGenerator: FlowGenerator[EntityRepr,EntityRepr]

  protected def warmUpFlow(g: Flow[I, O, Future[Done]]) =

    Flow.fromGraph(GraphDSL.create(g){ implicit b: GraphDSL.Builder[Future[Done]] ⇒ f: FlowShape[I, O] ⇒

      import GraphDSL.Implicits._

      val warmUpSource = b.add(EntityWiseNetworkingModule.getWarmupEntitySource[I](transformWarmupEntity(_)))
      val merge = b.add(MergePreferred[I](1))

      warmUpSource ~> merge.preferred
      merge.out ~> f.in


      FlowShape.of[I, O](merge.in(0), f.out)

    })

  override def flow(connectionID: ID) = warmUpFlow(StreamingFlowOps.getRoutingCompositeFlow[I,O,EntityRepr,EntityRepr](dependency.streamingActorsRegistry,
    connectionID, codec, flowGenerator).mapMaterializedValue(_._1))

}

trait EntityWiseNetworkingModule extends CompositeFlowNetworkingModule {

  override type EntityRepr = BE

  override implicit val ertag = ClassTag[EntityRepr](classOf[EntityRepr])

  override val flowGenerator = new FlowGenerator[EntityRepr, EntityRepr] {

    protected lazy val entityDefinitions: Map[String, EntityDefinition[E] forSome {type E <: BaseEntity[E]}] = EntityDefinition.all().groupBy(_.tag.runtimeClass.toString).mapValues(_.head)

    override def apply[E <: EntityRepr](flowID: ID)(implicit ct: ClassTag[E]) = Try {
      val io = entityDefinitions.get(ct.runtimeClass.toString).get.io
      Flow[E].map(x => (x, None).asInstanceOf[io.Repr]).via(io.ioFlow(protocol)).map(_._1)
    }

  }

}

trait ContextAwareEntityWiseNetworkingModule extends CompositeFlowNetworkingModule {

  override type EntityRepr = (BE, Option[String])

  override implicit val ertag = ClassTag[EntityRepr](classOf[EntityRepr])

  override val flowGenerator = new FlowGenerator[EntityRepr , EntityRepr] {

    protected lazy val entityDefinitions: Map[String, EntityDefinition[E] forSome {type E <: BaseEntity[E]}] = EntityDefinition.all().groupBy(_.tag.runtimeClass.toString).mapValues(_.head)

    override def apply[E <: EntityRepr](flowID: ID)(implicit ct: ClassTag[E]) = Try {
      entityDefinitions.get(ct.runtimeClass.toString).get.io.ioFlow(protocol).asInstanceOf[Flow[E, E, _]]}
  }

}

object EntityWiseNetworkingModule {

  def getWarmupEntitySource[T](transformer: BE  => Future[T])(implicit ec: ExecutionContext, mat: Materializer): Source[T, NotUsed] = {
    Source.fromIterator(() => EntityDefinition.all().map(EntityWiseNetworkingModule.createWarmupEntity(_).asInstanceOf[BE]).toIterator)
      .mapAsyncUnordered(4)(x => transformer(x).map[Option[T]](Some(_)).recover[Option[T]] { case x: Throwable => None }).collect[T] { case Some(x) => x }
  }

  def createWarmupEntity[E<:BaseEntity[E]](definition: EntityDefinition[E]): E = definition.nested.foldLeft(definition.newHollowInstance(""))((x,y) => y.setScalar(x, ""))

}

object NetworkingModule {

  def toS(a: InetSocketAddress) = {
    val addressString =a.getAddress.toString
    val suffix = ":" + a.getPort.toString
    if (addressString.startsWith("/")) (addressString.substring(1) + suffix) else if (addressString.startsWith("localhost/"))
      (addressString.substring(10) + suffix) else (addressString + suffix)
  }

  val clientFlowCategoryID = ID(ID.FlowCategory, "CL")

  val serverFlowCategoryID = ID(ID.FlowCategory, "SR")

  def getConnectionFlowID(clientAddress: InetSocketAddress, serverAddress: InetSocketAddress, isServer: Boolean, protocol: NetworkingProtocol): ID = {
    ID(if (isServer) serverFlowCategoryID else clientFlowCategoryID, ID(ID.ConnectionCategory, s"${toS(clientAddress)}>$protocol<${toS(serverAddress)}"))
  }

}