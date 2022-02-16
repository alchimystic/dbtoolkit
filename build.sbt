name := "dbtoolkit"

scalaVersion := "2.13.8"
version      := "0.1.0-SNAPSHOT"

libraryDependencies ++= Seq(
  "org.postgresql" % "postgresql" % "42.2.1",
  "com.github.kxbmap" %% "configs" % "0.6.1",
)
