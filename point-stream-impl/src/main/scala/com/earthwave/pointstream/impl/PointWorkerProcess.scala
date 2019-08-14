package com.earthwave.pointstream.impl

import akka.actor.ActorSystem

object PointWorkerProcess {

  def main(args : Array[String]) : Unit = {


    val argMap = args.map( arg => arg.split( "=")).map( tok => (tok(0),tok(1))).toMap

    val worker = argMap.getOrElse("-worker", throw new Exception(s"Parameter -worker={name} is missing."))
    val instance = argMap.getOrElse("-instance", throw new Exception(s"Parameter -instance={id} is missing."))
    val systemName = s"${worker}_$instance"

    println(s"Creating system name $systemName")

    val actorSystem = ActorSystem(systemName)

  }

}
