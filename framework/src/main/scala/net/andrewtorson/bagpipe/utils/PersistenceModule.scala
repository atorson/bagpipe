/*
 *
 * Author: Andrew Torson
 * Date: Aug 8, 2016
 */

package net.andrewtorson.bagpipe.utils


import java.util.{Date, UUID}

import scala.reflect.ClassTag
import scala.util.{Success, Try}

import com.github.mauricio.async.db.pool.PartitionedConnectionPool
import com.github.mauricio.async.db.postgresql.PostgreSQLConnection
import net.andrewtorson.bagpipe.entities.Timestamp
import net.andrewtorson.bagpipe.{ApplicationService, ApplicationServiceUnavailableException}
import io.getquill.{PostgresAsyncContext, PostgresAsyncContextConfig, SnakeCase}
import com.typesafe.config.Config
import org.joda.time.{DateTime, LocalDateTime}



trait PersistenceModule extends ApplicationService[PersistenceModule]{

	@throws(classOf[ApplicationServiceUnavailableException])
	def context : BagpipePostgreSQLContext

}

class BagpipePostgreSQLContext(val pool: PartitionedConnectionPool[PostgreSQLConnection]) extends PostgresAsyncContext[SnakeCase](pool) {

	def this(config: PostgresAsyncContextConfig) = this(config.pool)
	def this(config: Config) = this(PostgresAsyncContextConfig(config))

	implicit val timestampDecoder : Decoder[Timestamp] =
		decoder[Timestamp] {
			case d: DateTime => Timestamp(d.toDate.getTime)
			case d: LocalDateTime => Timestamp(d.toDate.getTime)
		  case d: Date =>  Timestamp(d.getTime)
		}

	implicit val timestampEncoder: Encoder[Timestamp] =
		encoder[Timestamp]{x: Timestamp => new DateTime(new Date(x.millis))}


	implicit class TimestampComparator(t1: Timestamp) {
		def between(from: Timestamp, to: Timestamp) = quote(infix"$t1 > $from and $t1 <= $to".as[Boolean])
	}

	override implicit val uuidDecoder: Decoder[UUID] = decoder[UUID]{ case s: String => UUID.fromString(s) }
	override implicit val uuidEncoder: Encoder[UUID] = encoder[UUID] { x: UUID => x.toString }
}



class PersistenceModuleImpl(configurationModule: ConfigurationModule) extends PersistenceModule {

	private var innerContext: Option[BagpipePostgreSQLContext] = None

	override val tag = ClassTag[PersistenceModule](classOf[PersistenceModule])

	override val abnormalTerminationCode = 1

	override def isRunning = innerContext.isDefined && innerContext.get.pool.isConnected

	override def start = if (!isRunning) this.synchronized{
	     Try{
				 innerContext = Some(new BagpipePostgreSQLContext(configurationModule.config.getConfig("quill")))
			 } match {
				 case Success(_) => isRunning
				 case _ => false
			 }
	} else {false}


	override def stop: Boolean = if (isRunning) this.synchronized{
		Try{
			val ctx = innerContext.get
			ctx.close()
			val result = !ctx.pool.isConnected
			if (result) innerContext = None
			result
		} match {
			case Success(b: Boolean) => b
			case _ => false
		}
	} else {false}

	 override def context = Try{
		  innerContext.get
	 } match {
		 case Success(x) => x
		 case _ => ApplicationService.throwApplicationServiceUnavailableException[PersistenceModule]
	 }

	 ApplicationService.register[PersistenceModule](this)
}

