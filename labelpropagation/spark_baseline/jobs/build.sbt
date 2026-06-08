ThisBuild / version := "0.1.0"
ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "spark-baseline",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.8" % Provided,
      "org.apache.spark" %% "spark-graphx" % "3.5.8" % Provided
    )
  )
