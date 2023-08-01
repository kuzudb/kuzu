.PHONY: release debug test benchmark all alldebug arrow clean clean-external clean-all

GENERATOR=
FORCE_COLOR=
NUM_THREADS=
TEST_JOBS=
SANITIZER_FLAG=
ROOT_DIR=$(CURDIR)

ifndef $(NUM_THREADS)
	NUM_THREADS=1
endif
export CMAKE_BUILD_PARALLEL_LEVEL=$(NUM_THREADS)

ifndef $(TEST_JOBS)
	TEST_JOBS=10
endif

ifeq ($(OS),Windows_NT)
	ifndef $(GEN)
		GEN=ninja
	endif
	SHELL := cmd.exe
	.SHELLFLAGS := /c
endif

ifeq ($(GEN),ninja)
	GENERATOR=-G "Ninja"
	FORCE_COLOR=-DFORCE_COLORED_OUTPUT=1
endif

ifeq ($(ASAN), 1)
	SANITIZER_FLAG=-DENABLE_ADDRESS_SANITIZER=TRUE -DENABLE_THREAD_SANITIZER=FALSE -DENABLE_UBSAN=FALSE
endif
ifeq ($(TSAN), 1)
	SANITIZER_FLAG=-DENABLE_ADDRESS_SANITIZER=FALSE -DENABLE_THREAD_SANITIZER=TRUE -DENABLE_UBSAN=FALSE
endif
ifeq ($(UBSAN), 1)
	SANITIZER_FLAG=-DENABLE_ADDRESS_SANITIZER=FALSE -DENABLE_THREAD_SANITIZER=TRUE -DENABLE_UBSAN=TRUE
endif

ifeq ($(OS),Windows_NT)
define mkdirp
	(if not exist "$(1)" mkdir "$(1)")
endef
else
define mkdirp
	mkdir -p $(1)
endef
endif

release:
	$(call mkdirp,build/release) && cd build/release && \
	cmake $(GENERATOR) $(FORCE_COLOR) $(SANITIZER_FLAG) -DCMAKE_BUILD_TYPE=Release ../.. && \
	cmake --build . --config Release

debug:
	$(call mkdirp,build/debug) && cd build/debug && \
	cmake $(GENERATOR) $(FORCE_COLOR) $(SANITIZER_FLAG) -DCMAKE_BUILD_TYPE=Debug ../.. && \
	cmake --build . --config Debug

all:
	$(call mkdirp,build/release) && cd build/release && \
	cmake $(GENERATOR) $(FORCE_COLOR) $(SANITIZER_FLAG) -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTS=TRUE -DBUILD_BENCHMARK=TRUE -DBUILD_NODEJS=TRUE ../.. && \
	cmake --build . --config Release

alldebug:
	$(call mkdirp,build/debug) && cd build/debug && \
	cmake $(GENERATOR) $(FORCE_COLOR) $(SANITIZER_FLAG) -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTS=TRUE -DBUILD_BENCHMARK=TRUE -DBUILD_NODEJS=TRUE ../.. && \
	cmake --build . --config Debug

benchmark:
	$(call mkdirp,build/release) && cd build/release && \
	cmake $(GENERATOR) $(FORCE_COLOR) $(SANITIZER_FLAG) -DCMAKE_BUILD_TYPE=Release -DBUILD_BENCHMARK=TRUE ../.. && \
	cmake --build . --config Release

nodejs:
	$(call mkdirp,build/release) && cd build/release && \
	cmake $(GENERATOR) $(FORCE_COLOR) $(SANITIZER_FLAG) -DCMAKE_BUILD_TYPE=Release -DBUILD_NODEJS=TRUE ../.. && \
	cmake --build . --config Release

java:
	$(call mkdirp,build/release) && cd build/release && \
	cmake $(GENERATOR) $(FORCE_COLOR) $(SANITIZER_FLAG) -DCMAKE_BUILD_TYPE=Release -DBUILD_JAVA=TRUE ../.. && \
	cmake --build . --config Release

test:
	$(call mkdirp,build/release) && cd build/release && \
	cmake $(GENERATOR) $(FORCE_COLOR) $(SANITIZER_FLAG) -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTS=TRUE ../.. && \
	cmake --build . --config Release
	cd $(ROOT_DIR)/build/release/test && \
	ctest --output-on-failure -j ${TEST_JOBS}

lcov:
	$(call mkdirp,build/release) && cd build/release && \
	cmake $(GENERATOR) $(FORCE_COLOR) $(SANITIZER_FLAG) -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTS=TRUE -DBUILD_NODEJS=TRUE -DBUILD_LCOV=TRUE ../.. && \
	cmake --build . --config Release
	cd $(ROOT_DIR)/build/release/test && \
	ctest --output-on-failure -j ${TEST_JOBS}

pytest:
	$(MAKE) release
	cd $(ROOT_DIR)/tools/python_api/test && \
	python3 -m pytest -v test_main.py

nodejstest:
	$(MAKE) nodejs
	cd $(ROOT_DIR)/tools/nodejs_api/ && \
	npm test

javatest: arrow
ifeq ($(OS),Windows_NT)
	$(MAKE) java
	$(call mkdirp,tools/java_api/build/test)  && cd tools/java_api/ && \
	javac -d build/test -cp ".;build/kuzu_java.jar;third_party/junit-platform-console-standalone-1.9.3.jar"  -sourcepath src/test/java/com/kuzudb/test/*.java && \
	java -jar third_party/junit-platform-console-standalone-1.9.3.jar -cp ".;build/kuzu_java.jar;build/test/" --scan-classpath --include-package=com.kuzudb.java_test --details=verbose
else
	$(MAKE) java
	$(call mkdirp,tools/java_api/build/test)  && cd tools/java_api/ && \
	javac -d build/test -cp ".:build/kuzu_java.jar:third_party/junit-platform-console-standalone-1.9.3.jar"  -sourcepath src/test/java/com/kuzudb/test/*.java && \
	java -jar third_party/junit-platform-console-standalone-1.9.3.jar -cp ".:build/kuzu_java.jar:build/test/" --scan-classpath --include-package=com.kuzudb.java_test --details=verbose
endif

rusttest:
ifeq ($(OS),Windows_NT)
	cd $(ROOT_DIR)/tools/rust_api && \
	set KUZU_TESTING=1 && \
	set CFLAGS=/MDd && \
	set CXXFLAGS=/MDd /std:c++20 && \
	cargo test --features arrow -- --test-threads=1
else
	cd $(ROOT_DIR)/tools/rust_api && \
	CARGO_BUILD_JOBS=$(NUM_THREADS) cargo test --features arrow -- --test-threads=1
endif

clean-python-api:
ifeq ($(OS),Windows_NT)
	if exist tools\python_api\build rmdir /s /q tools\python_api\build
else
	rm -rf tools/python_api/build
endif

clean-java:
ifeq ($(OS),Windows_NT)
	if exist tools\java_api\build rmdir /s /q tools\java_api\build
else
	rm -rf tools/java_api/build
endif

clean: clean-python-api clean-java
ifeq ($(OS),Windows_NT)
	if exist build rmdir /s /q build
else
	rm -rf build
endif
