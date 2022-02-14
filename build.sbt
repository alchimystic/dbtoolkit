import Dependencies.{scalaTest, _}
import sbt.Keys.libraryDependencies

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "zalando",
      scalaVersion := "2.12.4",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "dbtoolkit",

    libraryDependencies += "org.postgresql" % "postgresql" % "42.2.1",
    libraryDependencies += "com.squareup" % "kotlinpoet" % "0.7.0",
    libraryDependencies += "javax.persistence" % "javax.persistence-api" % "2.2",
    libraryDependencies += "org.hibernate" % "hibernate-core" % "5.2.15.Final",
    libraryDependencies += "com.vladmihalcea" % "hibernate-types-52" % "2.2.0",
    libraryDependencies += "org.typelevel" %% "cats-core" % "1.0.1",
    //libraryDependencies += "com.typesafe" % "config" % "1.3.2",
    libraryDependencies += "com.github.kxbmap" %% "configs" % "0.4.4",

    //    compile("com.vladmihalcea:hibernate-types-52:2.2.0")
    libraryDependencies += scalaTest % Test
  )
