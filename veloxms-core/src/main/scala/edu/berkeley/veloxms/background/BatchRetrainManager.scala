package edu.berkeley.veloxms.background

import java.util.Date
import java.util.concurrent.TimeUnit

import akka.pattern.ask
import akka.util.Timeout

import edu.berkeley.veloxms._
import edu.berkeley.veloxms.cluster.{ModelPartitionManager, ActorSystemManager}
import edu.berkeley.veloxms.models.Model
import edu.berkeley.veloxms.resources.internal.ModelPartitionManager
import edu.berkeley.veloxms.util.EtcdClient
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.concurrent.{Future, Await}
import scala.concurrent.duration._

class BatchRetrainManager[T](
    model: Model[T],
    hostPartitionMap: Seq[String],
    etcdClient: EtcdClient,
    sparkContext: SparkContext,
    sparkDataLocation: String,
    delay: Long,
    unit: TimeUnit)
  extends BackgroundTask(delay, unit) {

  implicit val timeout = Timeout(5 hours)
  import ActorSystemManager.system._

  override protected def execute(): Unit = batchTrain()

  /**
   *  Server:
   *    - Acquire global retrain lock (lock per model) from etcd
   *    - Tell servers to disable online updates
   *        * Don't forget, this should be done for the retrain master as well
   *    - Tell servers to write obs to HDFS - servers respond to request when done
   *        * Don't forget, this should be done for the retrain master as well
   *    - Retrain in Spark
   *    - When Spark retrain done, request servers load new model - servers respond when done
   *        * Don't forget, this should be done for the retrain master as well
   *    - Tell servers to re-enable online updates
   *        * Don't forget, this should be done for the retrain master as well
   *    - Release retrain lock
   *
   */
  def batchTrain(): String = {
    val modelName = model.modelName
    var retrainResult = ""
    // Really should load whatever port is in application.conf
    val veloxPort = 2552
    val actorPaths = hostPartitionMap.map(host => s"akka.tcp://${ActorSystemManager.systemName}@$host:$veloxPort/user/$modelName")
    val partitionManagers = actorPaths.map(path => ActorSystemManager.system.actorSelection(path))

    // coordinate retraining: returns false if retrain already going on
    logInfo(s"Starting retrain for model $modelName")
    val lockAcquired = etcdClient.acquireRetrainLock(modelName)

    if (lockAcquired) {
      try {
        val nextVersion = new Date().getTime
        val obsDataLocation = s"$sparkDataLocation/$modelName/observations/$nextVersion"
        val newModelLocation = s"$modelName/retrained_model/$nextVersion"

        // Disable online updates
        val disableOnlineUpdateRequests = partitionManagers.map(ask(_, ModelPartitionManager.DisableOnlineUpdates).mapTo[String])
        val disableOnlineUpdateFutures = Future.sequence(disableOnlineUpdateRequests)
        val disableOnlineUpdateResponses = Await.result(disableOnlineUpdateFutures, timeout.duration)
        logInfo(s"Disabled online updates: ${disableOnlineUpdateResponses.mkString("\n")}")

        // Write the observations to the spark server
        val writeRequests = partitionManagers.map(ask(_, ModelPartitionManager.WriteTrainingData(obsDataLocation)).mapTo[String])
        val writeResponseFutures = Future.sequence(writeRequests)
        val writeResponses = Await.result(writeResponseFutures, timeout.duration)
        logInfo(s"Write to spark cluster responses: ${writeResponses.mkString("\n")}")

        // Do the core batch retrain on spark
        val trainingData: RDD[(UserID, T, Double)] = sparkContext.objectFile(s"$obsDataLocation/*/*")

        val itemFeatures = model.retrainFeatureModelsInSpark(trainingData, nextVersion)
        val userWeights = model.retrainUserWeightsInSpark(itemFeatures, trainingData, nextVersion).map {
          case (userId, weights) => s"$userId, ${weights.mkString(", ")}"
        }

        userWeights.saveAsTextFile(s"$sparkDataLocation/$newModelLocation/users")
        logInfo("Finished retraining new model in spark")

        // Load the batch retrained models from spark & switch to the new models
        val loadModelRequests = partitionManagers.map(ask(_, ModelPartitionManager.LoadModel(newModelLocation, nextVersion)).mapTo[String])
        val loadResponseFutures = Future.sequence(loadModelRequests)
        val loadResponses = Await.result(loadResponseFutures, timeout.duration)
        logInfo(s"Load new model responses: ${loadResponses.mkString("\n")}")

        retrainResult = "Success"
      } finally {
        // Re-enable the online updates
        val enableOnlineUpdateRequests = partitionManagers.map(ask(_, ModelPartitionManager.EnableOnlineUpdates).mapTo[String])
        val enableOnlineUpdateFutures = Future.sequence(enableOnlineUpdateRequests)
        val enableOnlineUpdateResponses = Await.result(enableOnlineUpdateFutures, timeout.duration)
        logInfo(s"re-enabled online updates: ${enableOnlineUpdateResponses.mkString("\n")}")

        // Release the training lock
        val lockReleased = etcdClient.releaseRetrainLock(modelName)
        logInfo(s"released lock successfully: $lockReleased")
      }
    } else {
      retrainResult = "Failed to acquire lock"
    }

    logInfo(s"Result of batch retrain: $retrainResult")
    retrainResult
  }
}
