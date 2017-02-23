package it.teamDigitale

import java.security.Timestamp
import java.util.Date
import java.util.concurrent.{Executors, TimeUnit}

import com.sun.javafx.tk.Toolkit.Task
import org.slf4j.LoggerFactory

/**
  * Created by fabiana on 23/02/17.
  */
object ProducerMain extends App {

  //FIXEME we should add a redis db in the way to do not have redundant data if the service go down

  val logger = LoggerFactory.getLogger(this.getClass)

  var lastGeneratedTime: Option[Long] = None

  val ex = Executors.newScheduledThreadPool(1)

  val task = new Runnable {
    def run() = {
      lastGeneratedTime match {
        case None =>
          lastGeneratedTime = Some(TorinoTrafficProducer.run(-1L))
          logger.info(s"Data analyzed for the time ${lastGeneratedTime.get}")
        case Some(t) =>
          lastGeneratedTime = Some(TorinoTrafficProducer.run(t))
      }
    }
  }
  ex.scheduleAtFixedRate(task, 2, 5, TimeUnit.SECONDS)

}
