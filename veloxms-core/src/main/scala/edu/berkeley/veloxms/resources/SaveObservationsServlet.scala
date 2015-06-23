package edu.berkeley.veloxms.resources

import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

import com.codahale.metrics.Timer
import dispatch._
import edu.berkeley.veloxms._
import edu.berkeley.veloxms.cluster.HDFSLocation
import edu.berkeley.veloxms.util.Logging

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

class SaveObservationsServlet(
    timer: Timer,
    modelName: String,
    partitionMap: Seq[String],
    hostname: String) extends HttpServlet with Logging {
  private val http = Http.configure(_.setAllowPoolingConnection(true).setFollowRedirects(true))
  val veloxPort = 8080
  val hosts = partitionMap.map {
    h => host(h, veloxPort).setContentType("application/json", "UTF-8")
  }

  override def doPost(req: HttpServletRequest, resp: HttpServletResponse) {
    val timeContext = timer.time()
    try {
      val obsLocation = jsonMapper.readValue(req.getInputStream, classOf[HDFSLocation])

      // Write the observations to the spark server
      val writeRequests = hosts.map(
        h => {
          val req = (h / "writetrainingdata" / modelName)
              .POST << jsonMapper.writeValueAsString(obsLocation)
          http(req OK as.String)
        })

      val writeResponseFutures = Future.sequence(writeRequests)
      val writeResponses = Await.result(writeResponseFutures, Duration.Inf)
      logInfo(s"Saving observations to ${obsLocation.loc}: ${writeResponses.mkString("\n")}")

      resp.setContentType("application/json")
      jsonMapper.writeValue(resp.getOutputStream, "success")
    } finally {
      timeContext.stop()
    }
  }
}
