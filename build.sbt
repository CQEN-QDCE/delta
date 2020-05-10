/*
 * Copyright (2020) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

name := "delta-core"

organization := "io.delta"

scalaVersion := "2.12.10"

val sparkVersion = "3.0.0"

libraryDependencies ++= Seq(
  // Adding test classifier seems to break transitive resolution of the core dependencies
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % "provided",

  // Test deps
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "junit" % "junit" % "4.12" % "test",
  "com.novocode" % "junit-interface" % "0.11" % "test",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % "test" classifier "tests",
  "org.apache.spark" %% "spark-core" % sparkVersion % "test" classifier "tests",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "test" classifier "tests",

  // Compiler plugins
  // -- Bump up the genjavadoc version explicitly to 0.16 to work with Scala 2.12
  compilerPlugin("com.typesafe.genjavadoc" %% "genjavadoc-plugin" % "0.16" cross CrossVersion.full)
)

resolvers += "Temporary Staging of Spark 3.0" at "https://docs.delta.io/spark3artifacts/rc1/maven/"

enablePlugins(Antlr4Plugin)

antlr4Version in Antlr4 := "4.7"

antlr4PackageName in Antlr4 := Some("io.delta.sql.parser")

antlr4GenListener in Antlr4 := true

antlr4GenVisitor in Antlr4 := true

testOptions in Test += Tests.Argument("-oDF")

testOptions in Test += Tests.Argument(TestFrameworks.JUnit, "-v", "-a")

// Execute in parallel since run tests in a forked JVM (we don't need to worry about multiple Spark
// contexts in the same JVM)
Test / testGrouping := (Test / testGrouping).value.flatMap { group =>
  // SBT does parallelism and forking at the test group level. By default there is 1 test group per
  // sbt subproject. We take the existing test-groups, and for each test/suite make a new test group
  // containing the single suite (with forking enabled). This allows paralleism across all test
  // suites.
  group.tests.map(test => sbt.Tests.Group(test.name, Seq(test), sbt.Tests.SubProcess(ForkOptions())))
}

// We need to set concurrent restrictions to execute tests in parallel. By default SBT sets this to
// 1x parallelism for forked JVMs.
concurrentRestrictions := {
  // Default to number of processors / 3
  val defaultParallelism = java.lang.Runtime.getRuntime().availableProcessors() / 3
  // Allow users to specify parallelism with env var
  val testParallelismOpt = sys.env.get("DELTA_TEST_PARALLELISM").map(_.toInt)
  val parallelism = testParallelismOpt.getOrElse(defaultParallelism)

  val logger = sLog.value

  if (testParallelismOpt.isDefined) {
    logger.info(s"Tests will run with user specified ${parallelism}x Parallelism")
  } else {
    logger.info(s"Tests will run with default ${parallelism}x Parallelism")
  }

  Seq(Tags.limit(Tags.ForkedTestGroup, parallelism))
}

scalacOptions ++= Seq(
  "-target:jvm-1.8",
  "-P:genjavadoc:strictVisibility=true" // hide package private types and methods in javadoc
)

javaOptions += "-Xmx1024m"

fork in Test := true

// Configurations to speed up tests and reduce memory footprint
javaOptions in Test ++= Seq(
  "-Dspark.ui.enabled=false",
  "-Dspark.ui.showConsoleProgress=false",
  "-Dspark.databricks.delta.snapshotPartitions=2",
  "-Dspark.sql.shuffle.partitions=5",
  "-Ddelta.log.cacheSize=3",
  "-Dspark.sql.sources.parallelPartitionDiscovery.parallelism=5",
  "-Xmx1024m"
)

/** ********************
 * ScalaStyle settings *
 * *********************/

scalastyleConfig := baseDirectory.value / "scalastyle-config.xml"

lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")

compileScalastyle := scalastyle.in(Compile).toTask("").value

(compile in Compile) := ((compile in Compile) dependsOn compileScalastyle).value

lazy val testScalastyle = taskKey[Unit]("testScalastyle")

testScalastyle := scalastyle.in(Test).toTask("").value

(test in Test) := ((test in Test) dependsOn testScalastyle).value

/*********************
 *  MIMA settings    *
 *********************/

(test in Test) := ((test in Test) dependsOn mimaReportBinaryIssues).value

def getVersion(version: String): String = {
    version.split("\\.").toList match {
        case major :: minor :: rest => s"$major.$minor.0" 
        case _ => throw new Exception(s"Could not find previous version for $version.")
    }
}

mimaPreviousArtifacts := Set("io.delta" %% "delta-core" %  getVersion(version.value))
mimaBinaryIssueFilters ++= MimaExcludes.ignoredABIProblems


/*******************
 * Unidoc settings *
 *******************/

enablePlugins(GenJavadocPlugin, JavaUnidocPlugin, ScalaUnidocPlugin)

// Configure Scala unidoc
scalacOptions in(ScalaUnidoc, unidoc) ++= Seq(
  "-skip-packages", "org:com:io.delta.sql:io.delta.tables.execution",
  "-doc-title", "Delta Lake " + version.value.replaceAll("-SNAPSHOT", "") + " ScalaDoc"
)

// Configure Java unidoc
javacOptions in(JavaUnidoc, unidoc) := Seq(
  "-public",
  "-exclude", "org:com:io.delta.sql:io.delta.tables.execution",
  "-windowtitle", "Delta Lake " + version.value.replaceAll("-SNAPSHOT", "") + " JavaDoc",
  "-noqualifier", "java.lang",
  "-tag", "return:X",
  // `doclint` is disabled on Circle CI. Need to enable it manually to test our javadoc.
  "-Xdoclint:all"
)

// Explicitly remove source files by package because these docs are not formatted correctly for Javadocs
def ignoreUndocumentedPackages(packages: Seq[Seq[java.io.File]]): Seq[Seq[java.io.File]] = {
  packages
    .map(_.filterNot(_.getName.contains("$")))
    .map(_.filterNot(_.getCanonicalPath.contains("io/delta/sql")))
    .map(_.filterNot(_.getCanonicalPath.contains("io/delta/tables/execution")))
    .map(_.filterNot(_.getCanonicalPath.contains("spark")))
}

unidocAllSources in(JavaUnidoc, unidoc) := {
  ignoreUndocumentedPackages((unidocAllSources in(JavaUnidoc, unidoc)).value)
}

// Ensure unidoc is run with tests
(test in Test) := ((test in Test) dependsOn unidoc.in(Compile)).value


/***************************
 * Spark Packages settings *
 ***************************/

/* TODO: Re-enable once we get Spark Package working on SBT 1.x
spName := "databricks/delta-core"

spAppendScalaVersion := true

spIncludeMaven := true

spIgnoreProvided := true

packageBin in Compile := spPackage.value

sparkComponents := Seq("sql")
*/

/********************
 * Release settings *
 ********************/

publishMavenStyle := true

releaseCrossBuild := true

licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))

pomExtra :=
  <url>https://delta.io/</url>
    <scm>
      <url>git@github.com:delta-io/delta.git</url>
      <connection>scm:git:git@github.com:delta-io/delta.git</connection>
    </scm>
    <developers>
      <developer>
        <id>marmbrus</id>
        <name>Michael Armbrust</name>
        <url>https://github.com/marmbrus</url>
      </developer>
      <developer>
        <id>brkyvz</id>
        <name>Burak Yavuz</name>
        <url>https://github.com/brkyvz</url>
      </developer>
      <developer>
        <id>jose-torres</id>
        <name>Jose Torres</name>
        <url>https://github.com/jose-torres</url>
      </developer>
      <developer>
        <id>liwensun</id>
        <name>Liwen Sun</name>
        <url>https://github.com/liwensun</url>
      </developer>
      <developer>
        <id>mukulmurthy</id>
        <name>Mukul Murthy</name>
        <url>https://github.com/mukulmurthy</url>
      </developer>
      <developer>
        <id>tdas</id>
        <name>Tathagata Das</name>
        <url>https://github.com/tdas</url>
      </developer>
      <developer>
        <id>zsxwing</id>
        <name>Shixiong Zhu</name>
        <url>https://github.com/zsxwing</url>
      </developer>
    </developers>

bintrayOrganization := Some("delta-io")

bintrayRepository := "delta"

import ReleaseTransformations._

releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  publishArtifacts,
  setNextVersion,
  commitNextVersion
)
