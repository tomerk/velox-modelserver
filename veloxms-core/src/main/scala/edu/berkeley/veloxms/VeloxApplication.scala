package edu.berkeley.veloxms

import edu.berkeley.veloxms.models.ModelFactory
import edu.berkeley.veloxms.resources._
import edu.berkeley.veloxms.resources.internal._
import edu.berkeley.veloxms.storage._
import edu.berkeley.veloxms.models._
import io.dropwizard.Configuration
import io.dropwizard.Application
import io.dropwizard.setup.Bootstrap
import io.dropwizard.setup.Environment
import com.fasterxml.jackson.annotation.JsonProperty
import javax.validation.constraints.NotNull
import scala.collection.mutable
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import edu.berkeley.veloxms.util._
import org.eclipse.jetty.servlet.ServletHolder

class VeloxConfiguration extends Configuration {
  val sparkMaster: String = "NoSparkMaster"
  val hostname: String = "none"

  @(JsonProperty)("models")
  val modelFactories: Map[String, ModelFactory] = Map()
  // sparkMaster: String
  // whether to do preprocessing of dataset for testing purposes
  // reloadTachyon: Boolean,
  // rawDataLoc: String
}

object VeloxEntry {

  final def main(args: Array[String]) {
    new VeloxApplication().run(args)
  }

}

class VeloxApplication extends Application[VeloxConfiguration] with Logging {

  override def getName = "velox model server"

  override def initialize(bootstrap: Bootstrap[VeloxConfiguration]) {
    bootstrap.getObjectMapper.registerModule(new DefaultScalaModule()) 
  }

  override def run(conf: VeloxConfiguration, env: Environment) {
    val models = new mutable.HashMap[String, Model[_,_]]

    // this assumes that etcd is running on each velox server
    val etcdClient = new EtcdClient(conf.hostname, 4001, conf.hostname, new DispatchUtil)

    conf.modelFactories.foreach { case (name, modelFactory) => {
      val (model, partition, partitionMap) = modelFactory.build(env, conf.hostname)

      val predictServlet = new PointPredictionServlet(model, env.metrics().timer(name + "/predict/"))
      val topKServlet = new TopKPredictionServlet(model, env.metrics().timer(name + "/predict_top_k/"))
      val observeServlet = new AddObservationServlet(
          model,
          conf.sparkMaster,
          env.metrics().timer(name + "/observe/"))
      val writeHdfsServlet = new WriteToHDFSServlet(
          model,
          env.metrics().timer(name + "/observe/"),
          conf.sparkMaster,
          partition)
      val retrainServlet = new RetrainServlet(
          model,
          conf.sparkMaster,
          env.metrics().timer(name + "/retrain/"),
          etcdClient,
          name,
          partitionMap)
      val loadNewModelServlet = new LoadNewModelServlet(
          model, 
          env.metrics().timer(name + "/loadmodel/"),
          conf.sparkMaster)
      env.getApplicationContext.addServlet(new ServletHolder(predictServlet), "/predict/" + name)
      env.getApplicationContext.addServlet(new ServletHolder(topKServlet), "/predict_top_k/" + name)
      env.getApplicationContext.addServlet(new ServletHolder(observeServlet), "/observe/" + name)
      env.getApplicationContext.addServlet(new ServletHolder(retrainServlet), "/retrain/" + name)
      env.getApplicationContext.addServlet(new ServletHolder(writeHdfsServlet), "/writehdfs/" + name)
      env.getApplicationContext.addServlet(new ServletHolder(loadNewModelServlet), "/loadmodel/" + name)
      models.put(name, model)
    }}
    logInfo("Registered models: " + conf.modelFactories.keys.mkString(","))
    env.jersey().register(new ModelListResource(conf.modelFactories.keys.toSeq))
    env.jersey().register(new CacheHitResource(models.toMap))
  }
}





