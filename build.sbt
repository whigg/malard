organization in ThisBuild := "com.earthwave"
version in ThisBuild := "1.0-SNAPSHOT"

// the Scala version that will be used for cross-compiled libraries
scalaVersion in ThisBuild := "2.12.8"

val macwire = "com.softwaremill.macwire" %% "macros" % "2.3.0" % "provided"
val scalaTest = "org.scalatest" %% "scalatest" % "3.0.4" % Test
val mongo = "org.mongodb.scala" %% "mongo-scala-driver" % "2.6.0"



lazy val `malard` = (project in file("."))
  .aggregate(`catalogue-api`, `catalogue-impl`,`point-api`,`point-impl`,`environment-api`,`environment-impl`)

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
  .dependsOn(`catalogue-api`)

lazy val `point-api` = (project in file("point-api"))
  .settings(
    libraryDependencies ++= Seq(
      lagomScaladslApi
    )
  ).dependsOn(`catalogue-api`)

lazy val `point-impl` = (project in file("point-impl"))
  .enablePlugins(LagomScala)
  .settings(
    libraryDependencies ++= Seq(
      lagomScaladslPersistenceCassandra,
      lagomScaladslKafkaBroker,
      lagomScaladslTestKit,
      macwire,
      scalaTest
    )
  )
  .settings(lagomForkedTestSettings)
  .dependsOn(`point-api`,`catalogue-api`,`environment-api`)

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
      scalaTest
    )
  )
  .settings(lagomForkedTestSettings)
  .dependsOn(`environment-api`)

