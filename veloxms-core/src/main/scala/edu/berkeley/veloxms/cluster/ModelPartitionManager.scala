package edu.berkeley.veloxms.cluster

import akka.actor.{Actor, Props}
import edu.berkeley.veloxms._
import edu.berkeley.veloxms.background.OnlineUpdateManager
import edu.berkeley.veloxms.models.Model
import edu.berkeley.veloxms.util.{Logging, Utils}
import org.apache.spark.SparkContext

object ModelPartitionManager {
  case object DisableOnlineUpdates
  case object EnableOnlineUpdates
  case class DownloadBulkObservations(loc: String)
  case class LoadModel(userWeightsLoc: String, version: Version)
  case class WriteTrainingData(loc: String)

  def props[T, U](
      onlineUpdateManager: OnlineUpdateManager[T],
      model: Model[U],
      sparkContext: SparkContext,
      sparkDataLocation: String,
      partition: Int,
      numPartitions: Int): Props = {
    Props(new ModelPartitionManager(
      onlineUpdateManager,
      model,
      sparkContext,
      sparkDataLocation: String,
      partition,
      numPartitions))
  }
}

/**
 * Actor managing the model partition. Technically T & U should be the same type, but due to weird
 * reflection stuff in [[VeloxApplication]] that doesn't compile
 */
class ModelPartitionManager[T, U](
    onlineUpdateManager: OnlineUpdateManager[T],
    model: Model[U],
    sparkContext: SparkContext,
    sparkDataLocation: String,
    partition: Int,
    numPartitions: Int) extends Actor with Logging {
  import ModelPartitionManager._
  def receive = {
    case DisableOnlineUpdates =>
      onlineUpdateManager.disableOnlineUpdates()
      sender() ! "success"

    case EnableOnlineUpdates =>
      onlineUpdateManager.enableOnlineUpdates()
      sender() ! "success"

    case DownloadBulkObservations(loc: String) =>
      val observations = sparkContext.objectFile[(UserID, T, Double)](loc).filter { x =>
        val uid = x._1
        Utils.nonNegativeMod(uid.hashCode(), numPartitions) == partition
      }

      observations.collect().foreach { case (uid, item, score) =>
        onlineUpdateManager.addObservation(uid, item, score)
      }

      sender() ! "success"

    case LoadModel(userWeightsLoc: String, version: Version) =>
      val uri = s"$sparkDataLocation/$userWeightsLoc"

      // TODO only add users in this partition: if (userId % partNum == 0)
      val users = sparkContext.textFile(s"$uri/users/*").map(line => {
        val userSplits = line.split(", ")
        val userId = userSplits(0).toLong
        val userFeatures: Array[Double] = userSplits.drop(1).map(_.toDouble)
        (userId, userFeatures)
      }).collect().toMap

      if (users.size > 0) {
        val firstUser = users.head
        logInfo(s"Loaded new models for ${users.size} users. " +
            s"First one is:\n${firstUser._1}, ${firstUser._2.mkString(", ")}")
      }

      // TODO: Should make sure it's sufficiently atomic
      model.writeUserWeights(users, version)
      model.useVersion(version)

      sender() ! "success"

    case WriteTrainingData(loc: String) =>
      val uri = s"$loc/part_$partition"
      val observations = model.getObservationsAsRDD(sparkContext)
      observations.saveAsObjectFile(uri)
      sender() ! "success"
  }
}
