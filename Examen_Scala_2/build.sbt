ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "Examen_Scala_2"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.4.4",
  "org.apache.spark" %% "spark-sql" % "3.4.4",
  "org.scalatest" %% "scalatest" % "3.0.8" % Test
)