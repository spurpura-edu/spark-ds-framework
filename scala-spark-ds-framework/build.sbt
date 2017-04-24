name := "scala-spark-framework"

version := "1.0.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.0.2"

// Spark Dependency
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion ,
  "org.apache.spark" %% "spark-mllib" % sparkVersion
)

test in assembly := {}
    