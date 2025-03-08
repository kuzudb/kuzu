# Kuzu Java API

The Kuzu Java API is a Gradle project that provides a Java API to interact with the Kuzu platform. The build process of Gradle is configured to automatically build the JNI binding first and bundle it with the final JAR file.

## Requirements
Java 11 or higher

## Build

```bash
./gradlew build
```

## Run test

```bash
./gradlew test
```

## Run the example

```bash
cd example && gradle run
```
