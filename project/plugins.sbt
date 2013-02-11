resolvers += Resolver.url("artifactory",
  url("http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases"))(Resolver.ivyStylePatterns)

resolvers ++= Seq("Ooyala Nexus" at "http://nexus.ooyala.com/nexus/content/groups/public/")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.8.3")
