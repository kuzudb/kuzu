# Kùzu Java API

## Requirements
Java 11 or higher

## Build

```
cd ../.. && make java NUM_THREADS=X 
```

## Run test

```
cd ../.. && make javatest NUM_THREADS=X 
```

## Run `test.java` example

First, build the Java API as described above. Then, run the following command:

```
java -cp ".:build/kuzu_java.jar" test.java 
```
