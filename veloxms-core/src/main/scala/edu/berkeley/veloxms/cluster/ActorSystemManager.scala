package edu.berkeley.veloxms.cluster

import akka.actor.ActorSystem

/**
 * Actor system holder
 */
object ActorSystemManager {
  val systemName = "VeloxActorSystem"
  val system = ActorSystem(systemName)
}
