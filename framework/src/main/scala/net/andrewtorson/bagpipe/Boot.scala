/*
 *
 * Author: Andrew Torson
 * Date: Aug 8, 2016
 */

package net.andrewtorson.bagpipe




import scala.reflect.ClassTag
import scala.util.Success
import net.andrewtorson.bagpipe.entities._
import net.andrewtorson.bagpipe.utils.{BagpipeStreamingModule, _}
import com.typesafe.scalalogging.LazyLogging


trait ApplicationService[E<:ApplicationService[E]] {
  val tag: ClassTag[E]
  val abnormalTerminationCode: Int
  def isRunning: Boolean
  def start: Boolean
  def stop: Boolean
}

trait MonolithAppService extends ApplicationService[MonolithAppService]{
  // marker for the entire app
  override val tag = ClassTag[MonolithAppService](classOf[MonolithAppService])

  override val abnormalTerminationCode = 666
}

class ApplicationServiceUnavailableException(message: String) extends RuntimeException(message)

object ApplicationService extends MonolithAppService with LazyLogging{

  @throws(classOf[ApplicationServiceUnavailableException])
  def runService[E<:ApplicationService[E]](implicit ct: ClassTag[E]) = runGivenService[E](ApplicationService[E])

  @throws(classOf[ApplicationServiceUnavailableException])
  def runGivenService[E<:ApplicationService[E]](e:E) = {
    if (registry.outer.regContext.registeredItems.keySet.contains(id[E](e.tag))) {
      innerRunGivenService(e)
    } else throwApplicationServiceUnavailableException[E](e.tag)
  }

  private def innerRunGivenService(e: ApplicationService[_]): Boolean ={
    if (!e.isRunning && !e.start) {
      logger.error(s"Failed to launch application service [${e.tag}], shutting down the application...")
      Runtime.getRuntime.exit(e.abnormalTerminationCode)
      false
    } else {
      logger.info(s"Application service [${e.tag}] is running")
      true
    }
  }

  @throws(classOf[ApplicationServiceUnavailableException])
  def shutdownService[E<:ApplicationService[E]](implicit ct: ClassTag[E]) = shutdownGivenService[E](ApplicationService[E])

  @throws(classOf[ApplicationServiceUnavailableException])
  def shutdownGivenService[E<:ApplicationService[E]](e:E) = {
    if (registry.outer.regContext.registeredItems.keySet.contains(id[E](e.tag))) {
      innerShutdownGivenService(e)
    } else throwApplicationServiceUnavailableException[E](e.tag)
  }

  private def innerShutdownGivenService(e: ApplicationService[_]): Boolean = {
    if (e.isRunning && !e.stop){
      logger.error(s"Failed to stop application service [${e.tag}], shutting down the application...")
      Runtime.getRuntime.exit(e.abnormalTerminationCode)
      false
    } else {
      logger.info(s"Application service [${e.tag}] is shutdown")
      true
    }
  }

  override def isRunning = {
    val services = ApplicationService.registry.outer.regContext.registeredItems.values
    if (services.isEmpty) false else services.foldLeft(true)(_ && _.isRunning)
  }

  private def isPartiallyRunning = {
    val services = ApplicationService.registry.outer.regContext.registeredItems.values

    if (services.isEmpty) false else services.foldLeft(false)(_ || _.isRunning)
  }

  override def start = this.synchronized{
    if (isPartiallyRunning) stop
    val b = (Seq[ApplicationService[_]]() ++ ApplicationService.registry.outer.regContext.registeredItems.values)
      .sortBy(_.abnormalTerminationCode).foldLeft(true)(_ && innerRunGivenService(_))
    if (b) true else {
      stop
      false
    }
  }

  override def stop: Boolean = this.synchronized{
    (Seq[ApplicationService[_]]() ++ ApplicationService.registry.outer.regContext.registeredItems.values)
      .sortBy(-1*_.abnormalTerminationCode).foldLeft(true)(_ && innerShutdownGivenService(_))
  }

  private val registry = new ConcurrentMapSimpleRegistry[ApplicationService[_]](Reg.cbBuilder[ApplicationService[_]].build())

  implicit val applicationServiceOrdering = new Ordering[ApplicationService[_]]{
    override def compare(x: ApplicationService[_], y: ApplicationService[_]): Int = x.abnormalTerminationCode.compareTo(y.abnormalTerminationCode)
  }

  private val asParentID = ID(ID.UtilityCategory, "ED")

  private def id[E<:ApplicationService[E]](implicit ct: ClassTag[E]): ID = {
    ID(asParentID, ct.runtimeClass.toString)
  }

  @throws(classOf[ApplicationServiceUnavailableException])
  def apply[E<:ApplicationService[E]](implicit ct: ClassTag[E]): E =
    registry ? (Reg.Get[ApplicationService[_]], id(ct)) match {
      case Success(Some(x: ApplicationService[_])) if x.tag == ct => {
        x.asInstanceOf[E]
      }
      case _ => throwApplicationServiceUnavailableException[E]
    }


  def register[E<:ApplicationService[E]](e:E)(implicit ct: ClassTag[E]): Boolean =

    registry ? (if (isRunning) Reg.AddIfAbsent[ApplicationService[_]] else Reg.Add[ApplicationService[_]],
      id(ct), Some({() => Some(e)})) match {
      case Success(Some(_)) => {
        Runtime.getRuntime.addShutdownHook(new Thread(){
          override def run {
            e.stop
          }
        })
        true
      }
      case _ => false
    }


  def throwApplicationServiceUnavailableException[E<:ApplicationService[E]](implicit ct: ClassTag[E]) =
    throw new ApplicationServiceUnavailableException(s"Application service ${ct.runtimeClass} is yet unavailable")
}

object Bagpipe extends App with LazyLogging{

  //ToDO: need to re-factor to use Guice injection here. By default, Scala singletons are lazyly loaded at the first explicit code reference
  private val entityDefinitions = Seq[EntityDefinition[_]](AuditDef, StatisticDef)
  val modules = new ConfigurationModuleImpl with ActorModuleImpl

  launchServices

  private def launchServices = {
    for (x <- entityDefinitions) {
      logger.info(s"Loaded definition of entity[${x.tag}]")
    }
    //ToDO: need to re-factor to use Guice injection below. By default, Scala singletons are lazyly loaded at the first explicit code reference
    new PersistenceModuleImpl(modules)
    new BagpipeStreamingModule(new ServiceBusModuleImpl(modules))
    new HttpRestfulServerModuleProto(modules)
    new TcpServerModuleProto(modules)
    new BagpipeMessagingModule(modules)

    ApplicationService.start
  }

}