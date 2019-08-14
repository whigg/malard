organization in ThisBuild := "com.earthwave"
version in ThisBuild := "1.0-SNAPSHOT"

// the Scala version that will be used for cross-compiled libraries
scalaVersion in ThisBuild := "2.12.8"

val macwire = "com.softwaremill.macwire" %% "macros" % "2.3.0" % "provided"
val scalaTest = "org.scalatest" %% "scalatest" % "3.0.4" % Test
val mongo = "org.mongodb.scala" %% "mongo-scala-driver" % "2.6.0"
val gdal = "org.gdal" % "gdal" % "2.4.0"
val akkaRemote = "com.typesafe.akka" %% "akka-remote" % "2.5.22"



lazy val `malard` = (project in file("."))
  .aggregate(`catalogue-api`, `catalogue-impl`,`environment-api`,`environment-impl`,`mask-api`,`mask-impl`,`gridcellstats-api`,`gridcellstats-impl`,`projection-api`,`projection-impl`,`validation-stream-api`,`validation-stream-impl`,`point-stream-api`,`point-stream-impl`)

lazy val `catalogue-api` = (project in file("catalogue-api"))
  .settings(
    libraryDependencies ++= Seq(
      lagomScaladslApi
    )
  )

lazy val `catalogue-impl` = (project in file("catalogue-impl"))
  .enablePlugins(LagomScala)
  .settings(
    libraryDependencies ++= Seq(
      lagomScaladslPersistenceCassandra,
      lagomScaladslKafkaBroker,
      lagomScaladslTestKit,
      macwire,
      scalaTest,
      mongo
    )
  )
  .settings(lagomForkedTestSettings)
  .dependsOn(`catalogue-api`,`environment-api`)

lazy val `environment-api` = (project in file("environment-api"))
  .settings(
    libraryDependencies ++= Seq(
      lagomScaladslApi
    )
  )

lazy val `environment-impl` = (project in file("environment-impl"))
  .enablePlugins(LagomScala)
  .settings(
    libraryDependencies ++= Seq(
      lagomScaladslPersistenceCassandra,
      lagomScaladslKafkaBroker,
      lagomScaladslTestKit,
      macwire,
      scalaTest,
      mongo
    )
  )
  .settings(lagomForkedTestSettings)
  .dependsOn(`environment-api`)

lazy val `mask-api` = (project in file("mask-api"))
  .settings(
    libraryDependencies ++= Seq(
      lagomScaladslApi
    )
  )

lazy val `mask-impl` = (project in file("mask-impl"))
  .enablePlugins(LagomScala)
  .settings(
    libraryDependencies ++= Seq(
      lagomScaladslPersistenceCassandra,
      lagomScaladslKafkaBroker,
      lagomScaladslTestKit,
      macwire,
      scalaTest,
      mongo
    )
  )
  .settings(lagomForkedTestSettings)
  .dependsOn(`mask-api`,`environment-api`)
  
  lazy val `gridcellstats-api` = (project in file("gridcellstats-api"))
  .settings(
    libraryDependencies ++= Seq(
      lagomScaladslApi
    )
  ).dependsOn(`mask-api`)

  lazy val `gridcellstats-impl` = (project in file("gridcellstats-impl"))
  .enablePlugins(LagomScala)
  .settings(
    libraryDependencies ++= Seq(
      lagomScaladslPersistenceCassandra,
      lagomScaladslKafkaBroker,
      lagomScaladslTestKit,
      macwire,
      scalaTest,
      mongo
    )
  )
  .settings(lagomForkedTestSettings)
  .dependsOn(`gridcellstats-api`,`environment-api`)
  
   lazy val `projection-api` = (project in file("projection-api"))
  .settings(
    libraryDependencies ++= Seq(
      lagomScaladslApi
    )
  )

  lazy val `projection-impl` = (project in file("projection-impl"))
  .enablePlugins(LagomScala)
  .settings(
    libraryDependencies ++= Seq(
      lagomScaladslPersistenceCassandra,
      lagomScaladslKafkaBroker,
      lagomScaladslTestKit,
      macwire,
      scalaTest,
      mongo
    )
  )
  .settings(lagomForkedTestSettings)
  .dependsOn(`projection-api`,`environment-api`)

lazy val `validation-stream-api` = (project in file("validation-stream-api"))
  .settings(
    libraryDependencies ++= Seq(
      lagomScaladslApi
    )
  )

lazy val `validation-stream-impl` = (project in file("validation-stream-impl"))
  .enablePlugins(LagomScala)
  .settings(
    libraryDependencies ++= Seq(
      lagomScaladslTestKit,
      macwire,
      scalaTest
    )
  )
  .dependsOn(`validation-stream-api`)

lazy val `point-stream-api` = (project in file("point-stream-api"))
  .settings(
    libraryDependencies ++= Seq(
      lagomScaladslApi
    )
  ).dependsOn(`catalogue-api`)

lazy val `point-stream-impl` = (project in file("point-stream-impl"))
  .enablePlugins(LagomScala)
  .settings(
    libraryDependencies ++= Seq(
      lagomScaladslTestKit,
      macwire,
      scalaTest,
      gdal,
      akkaRemote
    )
  )
  .dependsOn(`point-stream-api`, `catalogue-api`,`environment-api`,`projection-api`)


assemblyMergeStrategy in assembly := {
  
  case "BUILD" => MergeStrategy.discard
	case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "netcdfAll-4.6.jar" => MergeStrategy.discard
  case _ => MergeStrategy.first
  }