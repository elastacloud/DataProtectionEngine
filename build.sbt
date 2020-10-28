import sbtrelease.ReleasePlugin.autoImport.ReleaseTransformations._

name := "dataprotection"
organization := "com.elastacloud"
description := "Elastacloud Data Protection Demo for Spark Summit 2020"
homepage := Some(url("https://www.bp.com"))
developers += Developer(id = "sandy-may", name = "Sandy May", email = "sandy@elastacloud.com", url = url("https://github.com/sandy-may"))

lazy val scala212 = "2.12.11"

val sparkVersion = "3.0.1"
val scalaTestVersion = "3.2.0-SNAP10"

// Add Spark dependencies
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-mllib" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-avro" % sparkVersion,
  "io.delta" %% "delta-core" % "0.7.0" % Provided,
  "com.databricks" % "dbutils-api_2.11" % "0.0.4" % Provided
)

// Setup test dependencies and configuration
parallelExecution in Test := false
fork in Test := true

libraryDependencies ++= Seq(
  "org.scalactic" %% "scalactic" % scalaTestVersion,
  "org.scalatest" %% "scalatest" % scalaTestVersion % Test,
  "com.holdenkarau" %% "spark-testing-base" % (sparkVersion + "_1.0.0") % Test
)

coverageEnabled := false
coverageOutputCobertura := true
coverageOutputHTML := true
coverageMinimum := 70
coverageFailOnMinimum := false
coverageHighlighting := true
coverageExcludedPackages := "IO.*;Main;"

releaseNextCommitMessage := s"Setting next version to ${(version in ThisBuild).value} [skip ci]"
releaseCommitMessage := s"Setting version to ${(version in ThisBuild).value} [skip ci]"

releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  ReleaseStep(action = releaseStepCommand("coverage"), enableCrossBuild = true),
  runTest,
  ReleaseStep(action = releaseStepCommand("coverageOff"), enableCrossBuild = true),
  ReleaseStep(action = releaseStepCommand("coverageReport"), enableCrossBuild = true),
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  ReleaseStep(action = releaseStepCommand("package"), enableCrossBuild = true),
  setNextVersion,
  commitNextVersion,
  pushChanges
)