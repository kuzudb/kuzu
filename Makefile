.PHONY: all alldebug release debug benchmark example java javatest nodejs \
	nodejstest python pytest rust rusttest test lcov clangd tidy \
	clangd-diagnostics install clean

CLANGD_DIAGNOSTIC_INSTANCES ?= 4
GENERATOR=
NUM_THREADS ?= 1
PREFIX ?= install
ROOT_DIR=$(CURDIR)
RUNTIME_CHECK_FLAG=
SANITIZER_FLAG=
TEST_JOBS ?= 10
WERROR_FLAG=

export CMAKE_BUILD_PARALLEL_LEVEL=$(NUM_THREADS)

ifeq ($(OS),Windows_NT)
	GEN ?= ninja
	SHELL := cmd.exe
	.SHELLFLAGS := /c
endif

ifeq ($(GEN),ninja)
	GENERATOR=-G "Ninja"
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
	RUNTIME_CHECK_FLAG=-DENABLE_RUNTIME_CHECKS=TRUE
endif
ifeq ($(WERROR), 1)
	WERROR_FLAG=-DENABLE_WERROR=TRUE
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

all:
	$(call mkdirp,build/release) && cd build/release && \
	cmake $(GENERATOR) $(SANITIZER_FLAG) $(WERROR_FLAG) $(RUNTIME_CHECK_FLAG) \
		-DBUILD_BENCHMARK=TRUE \
		-DBUILD_EXAMPLES=TRUE \
		-DBUILD_JAVA=TRUE \
		-DBUILD_NODEJS=TRUE \
		-DBUILD_PYTHON=TRUE \
		-DBUILD_TESTS=TRUE \
		-DCMAKE_BUILD_TYPE=Release \
		../.. && \
	cmake --build . --config Release

alldebug:
	$(call mkdirp,build/debug) && cd build/debug && \
	cmake $(GENERATOR) $(SANITIZER_FLAG) $(WERROR_FLAG) $(RUNTIME_CHECK_FLAG) \
		-DBUILD_BENCHMARK=TRUE \
		-DBUILD_EXAMPLES=TRUE \
		-DBUILD_JAVA=TRUE \
		-DBUILD_NODEJS=TRUE \
		-DBUILD_PYTHON=TRUE \
		-DBUILD_TESTS=TRUE \
		-DCMAKE_BUILD_TYPE=Debug \
		../.. && \
	cmake --build . --config Debug

release:
	$(call mkdirp,build/release) && cd build/release && \
	cmake $(GENERATOR) $(SANITIZER_FLAG) $(WERROR_FLAG) $(RUNTIME_CHECK_FLAG) -DCMAKE_BUILD_TYPE=Release ../.. && \
	cmake --build . --config Release

debug:
	$(call mkdirp,build/debug) && cd build/debug && \
	cmake $(GENERATOR) $(SANITIZER_FLAG) $(WERROR_FLAG) $(RUNTIME_CHECK_FLAG) -DCMAKE_BUILD_TYPE=Debug ../.. && \
	cmake --build . --config Debug

benchmark:
	$(call mkdirp,build/release) && cd build/release && \
	cmake $(GENERATOR) $(SANITIZER_FLAG) $(WERROR_FLAG) $(RUNTIME_CHECK_FLAG) -DCMAKE_BUILD_TYPE=Release -DBUILD_BENCHMARK=TRUE ../.. && \
	cmake --build . --config Release

example:
	$(call mkdirp,build/release) && cd build/release && \
	cmake $(GENERATOR) $(SANITIZER_FLAG) $(WERROR_FLAG) $(RUNTIME_CHECK_FLAG) -DCMAKE_BUILD_TYPE=Release -DBUILD_EXAMPLES=TRUE ../.. && \
	cmake --build . --config Release

java:
	$(call mkdirp,build/release) && cd build/release && \
	cmake $(GENERATOR) $(SANITIZER_FLAG) $(WERROR_FLAG) $(RUNTIME_CHECK_FLAG) -DCMAKE_BUILD_TYPE=Release -DBUILD_JAVA=TRUE ../.. && \
	cmake --build . --config Release

javatest: | java install
	java -jar tools/java_api/third_party/junit-platform-console-standalone-1.9.3.jar \
		-cp install/lib/java/kuzu_java.jar:build/release/tools/java_api/kuzu_java_test.jar \
		--scan-classpath --include-package=com.kuzudb.java_test --details=verbose

nodejs:
	$(call mkdirp,build/release) && cd build/release && \
	cmake $(GENERATOR) $(SANITIZER_FLAG) $(WERROR_FLAG) $(RUNTIME_CHECK_FLAG) -DCMAKE_BUILD_TYPE=Release -DBUILD_NODEJS=TRUE ../.. && \
	cmake --build . --config Release

nodejstest: | nodejs install
ifeq ($(OS),Windows_NT)
	cd $(ROOT_DIR)/tools/nodejs_api && \
	set NODEJS_KUZU_PATH=${ROOT_DIR}/install/lib/nodejs/kuzu && \
	npm test
else
	cd $(ROOT_DIR)/tools/nodejs_api && \
	NODEJS_KUZU_PATH=${ROOT_DIR}/install/lib/nodejs/kuzu npm test
endif

python:
	$(call mkdirp,build/release) && cd build/release && \
	cmake $(GENERATOR) $(SANITIZER_FLAG) $(WERROR_FLAG) $(RUNTIME_CHECK_FLAG) -DCMAKE_BUILD_TYPE=Release -DBUILD_PYTHON=TRUE ../.. && \
	cmake --build . --config Release

pytest: | python install
ifeq ($(OS),Windows_NT)
	cd $(ROOT_DIR)/tools/python_api/test && \
	set PYTHONPATH=${ROOT_DIR}/install/lib/python && \
	python3 -m pytest -v test_main.py
else
	cd $(ROOT_DIR)/tools/python_api/test && \
	PYTHONPATH=${ROOT_DIR}/install/lib/python python3 -m pytest -v test_main.py
endif

rust:
ifeq ($(OS),Windows_NT)
	cd $(ROOT_DIR)/tools/rust_api && \
	set KUZU_TESTING=1 && \
	set CFLAGS=/MDd && \
	set CXXFLAGS=/MDd /std:c++20 && \
	set CARGO_BUILD_JOBS=$(NUM_THREADS) && \
	cargo build --features arrow --
else
	cd $(ROOT_DIR)/tools/rust_api && \
	CARGO_BUILD_JOBS=$(NUM_THREADS) cargo build --features arrow
endif

rusttest: rust
	cd $(ROOT_DIR)/tools/rust_api && \
	cargo test -- --test-threads=$(TEST_JOBS)

test:
	$(call mkdirp,build/release) && cd build/release && \
	cmake $(GENERATOR) $(SANITIZER_FLAG) $(WERROR_FLAG) $(RUNTIME_CHECK_FLAG) -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTS=TRUE ../.. && \
	cmake --build . --config Release
	cd $(ROOT_DIR)/build/release/test && \
	ctest --output-on-failure -j ${TEST_JOBS}

lcov:
	$(call mkdirp,build/release) && cd build/release && \
	cmake $(GENERATOR) $(SANITIZER_FLAG) $(WERROR_FLAG) $(RUNTIME_CHECK_FLAG) -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTS=TRUE -DBUILD_NODEJS=TRUE -DBUILD_LCOV=TRUE ../.. && \
	cmake --build . --config Release
	cd $(ROOT_DIR)/build/release/test && \
	ctest --output-on-failure -j ${TEST_JOBS}

clangd:
	$(call mkdirp,build/release) && cd build/release && \
	cmake $(GENERATOR) $(SANITIZER_FLAG) $(WERROR_FLAG) $(RUNTIME_CHECK_FLAG) -DCMAKE_EXPORT_COMPILE_COMMANDS=1 ../..

tidy: clangd
	run-clang-tidy -p build/release -quiet -j $(NUM_THREADS) \
		"^$(realpath src)|$(realpath tools)/(?!shell/linenoise.cpp)|$(realpath examples)"

clangd-diagnostics: clangd
	find src -name *.h -or -name *.cpp | xargs \
		./scripts/get-clangd-diagnostics.py --compile-commands-dir build/release \
		-j $(NUM_THREADS) --instances $(CLANGD_DIAGNOSTIC_INSTANCES)

install:
	cmake --install build/release --prefix $(PREFIX) --strip

clean:
ifeq ($(OS),Windows_NT)
	if exist build rmdir /s /q build
else
	rm -rf build
endif
