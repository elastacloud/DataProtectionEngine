# Data Protection Demo

Data Protection targeting Scala 2.12 and Apache Spark 3.0.x

### Testing and coverage

Using the integrated tools in IntelliJ is the best option for running tests and generating code coverage. The solution
has been configured for use with JaCoCo to produce coverage results in the build pipeline. To use JaCoCo to generate
the code coverage information execute the following from the integrated SBT shell.

```bash
sbt> jacoco
```

### Releasing

The solution configuration makes use of [sbt-release](https://github.com/sbt/sbt-release) which allows the project 
to be compiled, tested, and deployed from a single command. This process has been configured in the `build.sbt` file. As 
part of the release process the plugin will auto-increment the version number, tag Git, update the next version, and 
push all changes back to git.

For working locally it is recommended to not use the `release` command, this is already configured in the build 
pipeline (see `azure-pipelines.yml`). Instead, use `package`