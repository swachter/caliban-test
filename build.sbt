name := "caliban-test"

version := "0.1"

scalaVersion := "2.13.3"

jooqVersion := "3.13.2"

// Enable the plugin
enablePlugins(JooqCodegenPlugin)

jooqCodegenConfig :=
  <configuration>
    <generator>
      <database>
        <name>org.jooq.meta.extensions.liquibase.LiquibaseDatabase</name>
        <properties>
          <property>
            <key>scripts</key>
            <value>src/main/resources/database.xml</value>
          </property>
          <property>
            <key>unqualifiedSchema</key>
            <value>PUBLIC</value>
          </property>
        </properties>
        <syntheticIdentities>public\..*\.id</syntheticIdentities>
      </database>
      <target>
        <packageName>ct.sql</packageName>
        <directory>target/jooq</directory>
      </target>
    </generator>
  </configuration>

managedSourceDirectories in Compile += baseDirectory.value / "target" / "jooq"

// Add your database driver dependency to `jooq-codegen` scope
libraryDependencies += "com.h2database" % "h2" % "1.4.200" % JooqCodegen
libraryDependencies += "org.jooq" % "jooq-meta-extensions" % jooqVersion.value % JooqCodegen
libraryDependencies += "org.liquibase" % "liquibase-core" % "3.10.3"

val zioVersion = "1.0.3"

libraryDependencies += "dev.zio" %% "zio" % zioVersion
libraryDependencies += "com.github.ghostdogpr" %% "caliban" % "0.9.4"

libraryDependencies += "com.h2database" % "h2" % "1.4.197" % Test
libraryDependencies += "org.scalatest" %% "scalatest" % "3.1.0" % Test
