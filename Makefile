.PHONY: release debug test benchmark all alldebug clean clean-all

CLANGD_DIAGNOSTIC_INSTANCES ?= 4
NUM_THREADS ?= 1
PREFIX ?= install
ROOT_DIR=$(CURDIR)
SANITIZER_FLAG=
TEST_JOBS ?= 10

export CMAKE_BUILD_PARALLEL_LEVEL=$(NUM_THREADS)

ifeq ($(OS),Windows_NT)
	GEN ?= ninja
	SHELL := cmd.exe
	.SHELLFLAGS := /c
endif

ifeq ($(GEN),ninja)
	CMAKE_FLAGS += -G "Ninja"
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

ifeq ($(RUNTIME_CHECKS), 1)
	CMAKE_FLAGS += -DENABLE_RUNTIME_CHECKS=TRUE
endif
ifeq ($(WERROR), 1)
	CMAKE_FLAGS += -DENABLE_WERROR=TRUE
endif
ifeq ($(LTO), 1)
	CMAKE_FLAGS += -DENABLE_LTO=TRUE
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
	cmake $(SANITIZER_FLAG) $(CMAKE_FLAGS) -DCMAKE_BUILD_TYPE=Release ../.. && \
	cmake --build . --config Release

debug:
	$(call mkdirp,build/debug) && cd build/debug && \
	cmake $(SANITIZER_FLAG) $(CMAKE_FLAGS) -DCMAKE_BUILD_TYPE=Debug ../.. && \
	cmake --build . --config Debug

all:
	$(call mkdirp,build/release) && cd build/release && \
	cmake $(SANITIZER_FLAG) $(CMAKE_FLAGS) -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTS=TRUE -DBUILD_BENCHMARK=TRUE -DBUILD_NODEJS=TRUE -DBUILD_EXAMPLES=TRUE ../.. && \
	cmake --build . --config Release

example:
	$(call mkdirp,build/release) && cd build/release && \
	cmake $(SANITIZER_FLAG) $(CMAKE_FLAGS) -DCMAKE_BUILD_TYPE=Release -DBUILD_EXAMPLES=TRUE ../.. && \
	cmake --build . --config Release

alldebug:
	$(call mkdirp,build/debug) && cd build/debug && \
	cmake $(SANITIZER_FLAG) $(CMAKE_FLAGS) -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTS=TRUE -DBUILD_BENCHMARK=TRUE -DBUILD_NODEJS=TRUE -DBUILD_EXAMPLES=TRUE ../.. && \
	cmake --build . --config Debug

benchmark:
	$(call mkdirp,build/release) && cd build/release && \
	cmake $(SANITIZER_FLAG) $(CMAKE_FLAGS) -DCMAKE_BUILD_TYPE=Release -DBUILD_BENCHMARK=TRUE ../.. && \
	cmake --build . --config Release

nodejs:
	$(call mkdirp,build/release) && cd build/release && \
	cmake $(SANITIZER_FLAG) $(CMAKE_FLAGS) -DCMAKE_BUILD_TYPE=Release -DBUILD_NODEJS=TRUE ../.. && \
	cmake --build . --config Release

java:
	$(call mkdirp,build/release) && cd build/release && \
	cmake $(SANITIZER_FLAG) $(CMAKE_FLAGS) -DCMAKE_BUILD_TYPE=Release -DBUILD_JAVA=TRUE ../.. && \
	cmake --build . --config Release

test:
	$(call mkdirp,build/release) && cd build/release && \
	cmake $(SANITIZER_FLAG) $(CMAKE_FLAGS) -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTS=TRUE ../.. && \
	cmake --build . --config Release
	cd $(ROOT_DIR)/build/release/test && \
	ctest --output-on-failure -j ${TEST_JOBS}

lcov:
	$(call mkdirp,build/release) && cd build/release && \
	cmake $(SANITIZER_FLAG) $(CMAKE_FLAGS) -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTS=TRUE -DBUILD_NODEJS=TRUE -DBUILD_LCOV=TRUE ../.. && \
	cmake --build . --config Release
	cd $(ROOT_DIR)/build/release/test && \
	ctest --output-on-failure -j ${TEST_JOBS}

clangd:
	$(call mkdirp,build/release) && cd build/release && \
	cmake $(SANITIZER_FLAG) $(CMAKE_FLAGS) -DCMAKE_EXPORT_COMPILE_COMMANDS=1 ../..

tidy: clangd
	run-clang-tidy -p build/release -quiet -j $(NUM_THREADS) \
		"^$(realpath src)|$(realpath tools)/(?!shell/linenoise.cpp)|$(realpath examples)"

clangd-diagnostics: clangd
	find src -name *.h -or -name *.cpp | xargs \
		./scripts/get-clangd-diagnostics.py --compile-commands-dir build/release \
		-j $(NUM_THREADS) --instances $(CLANGD_DIAGNOSTIC_INSTANCES)

pytest: release
	cd $(ROOT_DIR)/tools/python_api/test && \
	python3 -m pytest -v test_main.py

nodejstest: nodejs
	cd $(ROOT_DIR)/tools/nodejs_api/ && \
	npm test

javatest: java
ifeq ($(OS),Windows_NT)
	$(call mkdirp,tools/java_api/build/test)  && cd tools/java_api/ && \
	javac -d build/test -cp ".;build/kuzu_java.jar;third_party/junit-platform-console-standalone-1.9.3.jar" src/test/java/com/kuzudb/test/*.java && \
	java -jar third_party/junit-platform-console-standalone-1.9.3.jar -cp ".;build/kuzu_java.jar;build/test/" --scan-classpath --include-package=com.kuzudb.java_test --details=verbose
else
	$(call mkdirp,tools/java_api/build/test)  && cd tools/java_api/ && \
	javac -d build/test -cp ".:build/kuzu_java.jar:third_party/junit-platform-console-standalone-1.9.3.jar" src/test/java/com/kuzudb/test/*.java && \
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

install:
	cmake --install build/release --prefix $(PREFIX) --strip

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
