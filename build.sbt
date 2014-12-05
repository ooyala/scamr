import AssemblyKeys._

organization := "ooyala.scamr"

name := "scamr"

// Remove -SNAPSHOT from the version before publishing a release. Don't forget to change the version to
// $(NEXT_VERSION)-SNAPSHOT afterwards!
version := "0.3.5-SNAPSHOT"

scalaVersion := "2.10.4"

crossScalaVersions := Seq("2.10.4", "2.11.2")

// The hadoop artifact needs access to Cloudera's maven repo
resolvers += "Cloudera's CDH3 Maven repo" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

libraryDependencies ++= Seq(
  "joda-time" % "joda-time" % "2.3",
  "org.joda" % "joda-convert" % "1.5",
  "com.lambdaworks" %% "jacks" % "2.3.3",
  // Compiles against Cloudera's CDH5 hadoop distribution.
  // Note: since the hadoop dependency has "provided" scope, users of ScaMR will have to add a
  // CDH5-compatible hadoop-client to their library dependencies.
  // Include your own version of hadoop (which corresponds to the ScaMR version you're using), and make sure
  // to mark it as "provided" as well.
  // That way, 'sbt assembly' will not include all of hadoop's dependencies into your fat jar
  "org.apache.hadoop" % "hadoop-client" % "2.3.0-cdh5.1.0" % "provided"
)

// Subcut 2.9.3 is not there, so use 2.9.2 version for 2.9.3 and hope it works.
libraryDependencies <+= (scalaVersion) {
  case "2.9.3" => "com.escalatesoft.subcut" % "subcut_2.9.2" % "2.0"
  case _ => "com.escalatesoft.subcut" %% "subcut" % "2.1"
}


// This is to prevent error [java.lang.OutOfMemoryError: PermGen space]
javaOptions ++= Seq("-XX:MaxPermSize=1024m", "-Xmx2048m")

scalacOptions ++= Seq("-unchecked", "-deprecation")

// Note: to run the scamr examples, build a "fat jar" with sbt assembly.
// Then run the examples with the jar at target/scala-<scala version>/scamr-assembly-<scamr version>.jar
//
// To package scamr and use it as a library, use "sbt package". The library jar will be at
// target/scala-<scala version>/scamr_<scala version>-<scamr version>.jar
//
// To publish it to your own maven server, you'll need to have another .sbt file that defines the
// maven server location and credentials.
// TODO(ivmaykov): Publish ScaMR to some public maven repository.

// For the sbt-assembly plugin to be able to generate single JAR files for easy deploys
seq(sbtassembly.Plugin.assemblySettings: _*)

org.scalastyle.sbt.ScalastylePlugin.Settings

//************** Maven publishing settings ***************************

publishMavenStyle := true

// disable publishing the main API jar
publishArtifact in (Compile, packageDoc) := false
