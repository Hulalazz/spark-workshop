name := """spark-workshop"""

version := "1.0"

scalaVersion := "2.10.6"

val sparkVersion = "1.6.0"

//scope should be changed to "compile" when debugging in IDE
val sparkDepScope = "provided"

//ExclusionRules here to fix the jar hell when debugging applications in IDE
libraryDependencies ++= Seq(
   "org.apache.spark" %% "spark-core" % sparkVersion % sparkDepScope excludeAll ExclusionRule(organization = "javax.servlet"),
   "org.apache.spark" %% "spark-sql" % sparkVersion % sparkDepScope,
   "com.datastax.spark" %% "spark-cassandra-connector" % "1.5.0" excludeAll ExclusionRule(organization = "javax.servlet"),
   "org.mongodb.mongo-hadoop" % "mongo-hadoop-core" % "1.4.2" excludeAll ExclusionRule(organization = "javax.servlet"),
   "org.apache.hadoop" % "hadoop-hdfs" % "2.6.0" excludeAll ExclusionRule(organization = "javax.servlet"),
   "org.apache.hadoop" % "hadoop-client" % "2.6.0" excludeAll ExclusionRule(organization = "javax.servlet"),
   "org.mongodb" % "mongo-java-driver" % "3.2.2",
   "org.json4s" %% "json4s-jackson" % "3.3.0",
   "com.github.scopt" %% "scopt" % "3.3.0",
   "org.scalatest" %% "scalatest" % "2.2.4" % "test",
   "org.scalacheck" %% "scalacheck" % "1.12.2" % "test"
)

assemblyJarName in assembly := "spark-workshop.jar"

// There is a conflict between guava versions on Cassandra Driver and Hadoop
assemblyShadeRules in assembly := Seq(
   ShadeRule.rename("com.google.**" -> "shadeio.@1").inAll
)

assemblyMergeStrategy in assembly := {
   case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
   case _ => MergeStrategy.first
}