import scala.collection.Seq

name := "lab_notes"

version := "0.1"

scalaVersion := "2.13.16"

lazy val sparkVersion = "3.2.1"

lazy val circeVersion = "0.14.3"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "io.circe" %% "circe-core" % circeVersion,
  "io.circe" %% "circe-generic" % circeVersion,
  "io.circe" %% "circe-parser" % circeVersion,
  "com.fasterxml.jackson.core" % "jackson-core" % "2.15.3",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.15.3",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.15.3"
)