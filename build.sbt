import sbt.Keys.libraryDependencies

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "zalando",
      scalaVersion := "2.13.8",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "dbtoolkit",

    libraryDependencies += "org.postgresql" % "postgresql" % "42.2.1",
    libraryDependencies += "com.github.kxbmap" %% "configs" % "0.6.1",

  )
