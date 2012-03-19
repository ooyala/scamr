organization := "ooyala.scamr"

name := "scamr"

// Remove -SNAPSHOT from the version before publishing a release. Don't forget to change the version to
// $(NEXT_VERSION)-SNAPSHOT afterwards!
version := "0.1.0-SNAPSHOT"

scalaVersion := "2.9.1"

// sbt defaults to <project>/src/test/{scala,java} unless we put this in
unmanagedSourceDirectories in Test <<= Seq(baseDirectory(_ / "src" / "test")).join

unmanagedSourceDirectories in Compile <<= Seq( baseDirectory(_ / "src" / "main")).join

// Compile against Cloudera's CDH3u1 distro by default
libraryDependencies += "org.apache.hadoop" % "hadoop-core" % "0.20.2-cdh3u1"

// The above artifact needs access to Cloudera's maven repo
resolvers += "Cloudera's CDH3 Maven repo" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

libraryDependencies += "commons-logging" % "commons-logging" % "1.0.4"

libraryDependencies += "commons-codec" % "commons-codec" % "1.4"

// Note: joda-time 2.0 seems to have some problems loading
libraryDependencies += "joda-time" % "joda-time" % "1.6.2"

// This is to prevent error [java.lang.OutOfMemoryError: PermGen space]
javaOptions += "-XX:MaxPermSize=1024m"

javaOptions += "-Xmx2048m"

seq(proguardSettings: _*)

proguardOptions ++= Seq("-keep class scamr.** { *; }")

proguardLibraryJars <++= (update) map (_.select(module = moduleFilter(name = "servlet-api")))

minJarPath <<= target(_ / "scamr.jar")

// TODO(cespare/ivmaykov): Look into excluding unnecessary stuff from the jar now that we're using proguard
// NOTE(caleb): The generated jar is 8.8M, which is (a) already sufficiently small and (b) a little smaller
// than the generated jar was previously (when we used sbt-assembly and excluded a bunch of stuff).

// Enable the Scala Code Coverage Tool.  To run, do sbt coverage:test
// seq(ScctPlugin.scctSettings: _*)

// TODO(ivmaykov): Add unit tests!
// libraryDependencies += "org.scalatest" % "scalatest_2.9.0" % "1.6.1"

// libraryDependencies += "org.mockito" % "mockito-all" % "1.9.0-rc1"

//************** Maven publishing settings ***************************

publishMavenStyle := true

// disable publishing the main API jar
publishArtifact in (Compile, packageDoc) := false
