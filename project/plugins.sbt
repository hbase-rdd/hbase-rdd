addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "3.9.5")

addSbtPlugin("com.jsuereth" % "sbt-pgp" % "2.0.1")

resolvers += "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/"
