/*
 *
 * Author: Andrew Torson
 * Date: Aug 22, 2016
 */

package net.andrewtorson.bagpipe.utils

import scala.concurrent.{Await, Future}
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Keep, RunnableGraph}
import akka.stream.{ActorMaterializer, KillSwitches, UniqueKillSwitch}
import net.andrewtorson.bagpipe.messaging.MQ
import net.andrewtorson.bagpipe.networking.IO
import net.andrewtorson.bagpipe.{ApplicationService, ApplicationServiceUnavailableException}
import io.scalac.amqp._

/**
 * Created by Andrew Torson on 8/22/16.
 */

trait MessagingModule extends ApplicationService[MessagingModule]{

    @throws(classOf[ApplicationServiceUnavailableException])
    def connection: Connection

}

trait MessagingModuleImpl extends MessagingModule {

    import scala.concurrent.duration._

    protected val dependency: ConfigurationModule with ActorModule

    protected def getRunnableMQFlow[E<:BaseEntity[E]](entityDefinition: EntityDefinition[E]): RunnableGraph[UniqueKillSwitch]

    override val tag = ClassTag[MessagingModule](classOf[MessagingModule])
    override val abnormalTerminationCode = 8

    private val killSwitches = new ConcurrentMapSimpleRegistry[UniqueKillSwitch](Reg.cbBuilder[UniqueKillSwitch].build())

    protected var innerConnection: Option[Connection] = None

    implicit lazy val actorSystem: ActorSystem = dependency.system
    implicit lazy val materializer = ActorMaterializer()
    implicit lazy val ec = actorSystem.dispatcher

    override def isRunning = innerConnection.isDefined

    protected def bindQueue(queue: => Queue, exchange: Exchange, routingKey: => String)(implicit conn: Connection) =
        Try{connection.queueDeclare(queue).flatMap(_ => connection.queueBind(queue.name, exchange.name, routingKey))} match {
            case Success(x) => x
            case Failure(f) => Future.failed[Queue.BindOk](f)
        }

    protected def safe[T](future: Future[T]): Future[Option[T]] = future.map[Option[T]](Some(_)).recover[Option[T]]{case x: Throwable => None}

    protected def setupQueues(implicit conn: Connection) = {
        Future.sequence(Seq(conn.exchangeDeclare(MQ.inboundExchange),conn.exchangeDeclare(MQ.outboundExchange))).flatMap(_ =>
            Future.sequence(EntityDefinition.all().toSeq.map(x => safe[Seq[Queue.BindOk]](Future.sequence(Seq(
                bindQueue(x.mq.inboundQueue, MQ.inboundExchange, x.mq.routingKey),
                bindQueue(x.mq.outboundQueue, MQ.outboundExchange, x.mq.routingKey)
            ))))))
    }

    override def start = if (!isRunning) this.synchronized{
        Try{
            innerConnection = Some(Connection(dependency.config))
            Await.ready(setupQueues(innerConnection.get), 5.seconds)
            EntityDefinition.all().map(x => Try{(x.categoryID, getRunnableMQFlow(x))}).collect{case Success(x) => x}.foldLeft[Boolean](true)((b,x) =>
                b && (killSwitches ++ (Reg.Add[UniqueKillSwitch], x._1, x._2.run())))
        } match {
            case Success(b) => {
                if (!b) stop
                b
            }
            case _ => {
                stop
                false
            }
        }
    } else {false}


    override def stop: Boolean = if (isRunning) this.synchronized{
        Try{
            killSwitches.outer.regContext.registeredItems.values.foreach(_.shutdown())
            Await.ready(innerConnection.get.shutdown(), 5.seconds)
            innerConnection = None
        } match {
            case Success(_) => true
            case _ => false
        }
    } else {false}

    override def connection = Try{
        innerConnection.get
    } match {
        case Success(x) => x
        case _ => ApplicationService.throwApplicationServiceUnavailableException[MessagingModule]
    }

}

class BagpipeMessagingModule(override protected val dependency: ConfigurationModule with ActorModule) extends MessagingModuleImpl{

    override protected def getRunnableMQFlow[E <: BaseEntity[E]](entityDefinition: EntityDefinition[E]) = {
        val mq = entityDefinition.mq
        mq.inboundSource.viaMat(KillSwitches.single)(Keep.right).via(entityDefinition.io.ioFlow(IO.MQ_PROTOCOL)).to(mq.outboundSink)
    }

    ApplicationService.register[MessagingModule](this)

}


class ExternalPeerMessagingModuleImpl(override protected val dependency: ConfigurationModule with ActorModule) extends MessagingModuleImpl{


    override protected def getRunnableMQFlow[E <: BaseEntity[E]](entityDefinition: EntityDefinition[E]) = {
        val mq = entityDefinition.mq
        mq.outboundSource.viaMat(KillSwitches.single)(Keep.right).via(entityDefinition.io.ioFlow(IO.MQ_PROTOCOL)).to(mq.inboundSink)
    }


    ApplicationService.register[MessagingModule](this)

}

