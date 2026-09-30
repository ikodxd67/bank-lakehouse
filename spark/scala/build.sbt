// Версии совпадают с кластером: Spark 3.5.9 собран на Scala 2.12.18
ThisBuild / scalaVersion := "2.12.18"
ThisBuild / organization := "bank"

lazy val root = (project in file("."))
  .settings(
    name := "bank-fact",
    version := "0.1.0",
    libraryDependencies ++= Seq(
      // provided: jar-ы Spark уже есть на кластере и в jar задачи не попадают
      "org.apache.spark" %% "spark-sql" % "3.5.9" % Provided,
      "org.scalatest" %% "scalatest" % "3.2.19" % Test
    ),
    // тесты поднимают локальный Spark: отдельная JVM и открытые модули Java 17
    Test / fork := true,
    Test / javaOptions ++= Seq(
      "-Xmx1g",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
      "-Duser.timezone=Asia/Almaty"
    )
  )
