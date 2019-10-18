# ⛓ sandow

`sandow` bridges the Java Collections Framework and Elasticsearch indices.

## Deprecation notice

**In light of recent developments with Elasticsearch's [Java High Level REST Client](https://www.elastic.co/guide/en/elasticsearch/client/java-rest/master/java-rest-high.html) and the shutdown of [Ironsift](https://ironsift.com/), this library is no longer maintained.**

## Latest release

[![Release](https://jitpack.io/v/thunken/sandow.svg?style=flat-square)](https://github.com/thunken/sandow/releases)

To add a dependency on this project using Gradle, Maven, sbt, or Leiningen, we recommend using [JitPack](https://jitpack.io/#thunken/sandow/1.1.0). The Maven group ID is `com.github.thunken`, and the artifact ID is `sandow`.

For example, for Maven, first add the JitPack repository to your build file:
```xml
	<repositories>
		<repository>
		    <id>jitpack.io</id>
		    <url>https://jitpack.io</url>
		</repository>
	</repositories>
```

And then add the dependency:
```xml
	<dependency>
	    <groupId>com.github.thunken</groupId>
	    <artifactId>sandow</artifactId>
	    <version>1.1.0</version>
	</dependency>
```
