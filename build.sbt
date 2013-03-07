import AssemblyKeys._

organization := "ooyala.scamr"

name := "scamr"

// Remove -SNAPSHOT from the version before publishing a release. Don't forget to change the version to
// $(NEXT_VERSION)-SNAPSHOT afterwards!
version := "0.1.6-SNAPSHOT"

scalaVersion := "2.9.1"

crossScalaVersions := Seq("2.9.1", "2.10.0")

// sbt defaults to <project>/src/test/{scala,java} unless we put this in
unmanagedSourceDirectories in Test <<= Seq(baseDirectory(_ / "src" / "test")).join

unmanagedSourceDirectories in Compile <<= Seq( baseDirectory(_ / "src" / "main")).join

// The above artifact needs access to Cloudera's maven repo
resolvers += "Cloudera's CDH3 Maven repo" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

// Compile against Cloudera's CDH3u4 distro by default
libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-core" % "0.20.2-cdh3u4" % "provided",
  "commons-logging" % "commons-logging" % "1.0.4",
  "commons-codec" % "commons-codec" % "1.4",
  "joda-time" % "joda-time" % "1.6.2"    // Note: joda-time 2.0 seems to have some problems loading
)

// This is to prevent error [java.lang.OutOfMemoryError: PermGen space]
javaOptions += "-XX:MaxPermSize=1024m"

javaOptions += "-Xmx2048m"

// Note: to run the scamr examples, build a "fat jar" with sbt assembly.
// The run the examples with the jar at target/scamr-examples.jar
//
// To package scamr and use it as a library, use "sbt package". The library jar will be at
// target/scala-<scala version/scamr_<scala version>-<scamr version>.jar
//
// To publish it to your own maven server, you'll need to have another .sbt file that defines the
// maven server location and credentials.
// TODO(ivmaykov): Publish ScaMR to some public maven repository.

// For the sbt-assembly plugin to be able to generate single JAR files for easy deploys
seq(sbtassembly.Plugin.assemblySettings: _*)

jarName in assembly := "scamr-examples.jar"

excludedJars in assembly <<= (fullClasspath in assembly) map { _ filter { cp =>
    List("scala-compiler").exists(cp.data.getName.startsWith(_))
  }
}

//************** Maven publishing settings ***************************

publishMavenStyle := true

// disable publishing the main API jar
publishArtifact in (Compile, packageDoc) := false
