.PHONY: \
	release debug all alldebug \
	test lcov \
	java nodejs python rust \
	javatest nodejstest pytest rusttest \
	benchmark example \
	clangd tidy clangd-diagnostics \
	install \
	clean-extension clean-python-api clean-java clean \
	extension-test shell-test

.ONESHELL:
.SHELLFLAGS = -ec

CLANGD_DIAGNOSTIC_INSTANCES ?= 4
NUM_THREADS ?= 1
PREFIX ?= install
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

ifdef ASAN
	CMAKE_FLAGS += -DENABLE_ADDRESS_SANITIZER=$(ASAN)
endif
ifdef TSAN
	CMAKE_FLAGS += -DENABLE_THREAD_SANITIZER=$(TSAN)
endif
ifdef UBSAN
	CMAKE_FLAGS += -DENABLE_UBSAN=$(UBSAN)
endif

ifdef RUNTIME_CHECKS
	CMAKE_FLAGS += -DENABLE_RUNTIME_CHECKS=$(RUNTIME_CHECKS)
endif
ifdef WERROR
	CMAKE_FLAGS += -DENABLE_WERROR=$(WERROR)
endif
ifdef LTO
	CMAKE_FLAGS += -DENABLE_LTO=$(LTO)
endif

ifdef SKIP_SINGLE_FILE_HEADER
	CMAKE_FLAGS += -DBUILD_SINGLE_FILE_HEADER=FALSE
endif

# Must be first in the Makefile so that it is the default target.
release:
	$(call run-cmake-release,)

relwithdebinfo:
	$(call run-cmake-relwithdebinfo,)

debug:
	$(call run-cmake-debug,)

allconfig:
	$(call config-cmake-release, \
		-DBUILD_BENCHMARK=TRUE \
		-DBUILD_EXAMPLES=TRUE \
		-DBUILD_EXTENSIONS="httpfs;duckdb;postgres" \
		-DBUILD_JAVA=TRUE \
		-DBUILD_NODEJS=TRUE \
		-DBUILD_PYTHON=TRUE \
		-DBUILD_SHELL=TRUE \
		-DBUILD_TESTS=TRUE \
	)

all: allconfig
	$(call build-cmake-release)

alldebug:
	$(call run-cmake-debug, \
		-DBUILD_BENCHMARK=TRUE \
		-DBUILD_EXAMPLES=TRUE \
		-DBUILD_EXTENSIONS="httpfs;duckdb;postgres" \
		-DBUILD_JAVA=TRUE \
		-DBUILD_NODEJS=TRUE \
		-DBUILD_PYTHON=TRUE \
		-DBUILD_SHELL=TRUE \
		-DBUILD_TESTS=TRUE \
	)


# Main tests
test:
	$(call run-cmake-relwithdebinfo, -DBUILD_TESTS=TRUE -DENABLE_BACKTRACES=TRUE)
	ctest --test-dir build/relwithdebinfo/test --output-on-failure -j ${TEST_JOBS}

lcov:
	$(call run-cmake-release, -DBUILD_TESTS=TRUE -DBUILD_LCOV=TRUE)
	ctest --test-dir build/release/test --output-on-failure -j ${TEST_JOBS}


# Language APIs

# Required for clangd-related tools.
java_native_header:
	cmake --build build/release --target kuzu_java

java:
	$(call run-cmake-release, -DBUILD_JAVA=TRUE)

nodejs:
	$(call run-cmake-release, -DBUILD_NODEJS=TRUE)

python:
	$(call run-cmake-release, -DBUILD_PYTHON=TRUE)

python-debug:
	$(call run-cmake-debug, -DBUILD_PYTHON=TRUE)

rust:
ifeq ($(OS),Windows_NT)
	set KUZU_TESTING=1
	set CFLAGS=/MDd
	set CXXFLAGS=/MDd /std:c++20
	set CARGO_BUILD_JOBS=$(NUM_THREADS)
else
	export CARGO_BUILD_JOBS=$(NUM_THREADS)
endif
	cd tools/rust_api && cargo build --all-features


# Language API tests
javatest: java
	cmake -E make_directory tools/java_api/build/test
ifeq ($(OS),Windows_NT)
	cd tools/java_api &&\
	javac -d build/test -cp ".;build/kuzu_java.jar;third_party/junit-platform-console-standalone-1.9.3.jar" src/test/java/com/kuzudb/test/*.java &&\
	java -jar third_party/junit-platform-console-standalone-1.9.3.jar -cp ".;build/kuzu_java.jar;build/test/" --scan-classpath --include-package=com.kuzudb.java_test --details=verbose
else
	cd tools/java_api &&\
	javac -d build/test -cp ".:build/kuzu_java.jar:third_party/junit-platform-console-standalone-1.9.3.jar" src/test/java/com/kuzudb/test/*.java &&\
	java -jar third_party/junit-platform-console-standalone-1.9.3.jar -cp ".:build/kuzu_java.jar:build/test/" --scan-classpath --include-package=com.kuzudb.java_test --details=verbose
endif

nodejstest: nodejs
	cd tools/nodejs_api && npm test

pytest: python
	cmake -E env PYTHONPATH=tools/python_api/build python3 -m pytest -vv tools/python_api/test

pytest-debug: python-debug
	cmake -E env PYTHONPATH=tools/python_api/build python3 -m pytest -vv tools/python_api/test

rusttest: rust
	cd tools/rust_api && cargo test --locked --all-features -- --test-threads=1

# Other misc build targets
benchmark:
	$(call run-cmake-release, -DBUILD_BENCHMARK=TRUE)

example:
	$(call run-cmake-release, -DBUILD_EXAMPLES=TRUE)

extension-test:
	$(call run-cmake-relwithdebinfo, \
		-DBUILD_EXTENSIONS="httpfs;duckdb;postgres" \
		-DBUILD_EXTENSION_TESTS=TRUE \
		-DENABLE_ADDRESS_SANITIZER=TRUE \
		-DENABLE_BACKTRACES=TRUE \
	)
	ctest --test-dir build/relwithdebinfo/extension --output-on-failure -j ${TEST_JOBS}
	aws s3 rm s3://kuzu-dataset-us/${RUN_ID}/ --recursive

extension-debug:
	$(call run-cmake-debug, \
		-DBUILD_EXTENSIONS="httpfs;duckdb;postgres" \
		-DBUILD_KUZU=FALSE \
	)

extension-release:
	$(call run-cmake-release, \
		-DBUILD_EXTENSIONS="httpfs;duckdb;postgres" \
		-DBUILD_KUZU=FALSE \
	)

shell-test:
	$(call run-cmake-relwithdebinfo, \
		-DBUILD_SHELL=TRUE \
	)
	cd tools/shell/test && python3 -m pytest -v

# Clang-related tools and checks

# Must build the java native header to avoid missing includes. Pipe character
# `|` ensures these targets build in this order, even in the presence of
# parallelism.
tidy: | allconfig java_native_header
	run-clang-tidy -p build/release -quiet -j $(NUM_THREADS) \
		"^$(realpath src)|$(realpath extension)|$(realpath tools)/(?!shell/linenoise.cpp)"

tidy-analyzer: | allconfig java_native_header
	run-clang-tidy -config-file .clang-tidy-analyzer -p build/release -quiet -j $(NUM_THREADS) \
		"^$(realpath src)|$(realpath extension)|$(realpath tools)/(?!shell/linenoise.cpp)"

clangd-diagnostics: | allconfig java_native_header
	find src -name *.h -or -name *.cpp | xargs \
		./scripts/get-clangd-diagnostics.py --compile-commands-dir build/release \
		-j $(NUM_THREADS) --instances $(CLANGD_DIAGNOSTIC_INSTANCES)


# Installation
install:
	cmake --install build/release --prefix $(PREFIX)


# Cleaning
clean-extension:
	cmake -E rm -rf extension/httpfs/build
	cmake -E rm -rf extension/duckdb/build
	cmake -E rm -rf extension/postgres/build

clean-python-api:
	cmake -E rm -rf tools/python_api/build

clean-java:
	cmake -E rm -rf tools/java_api/build

clean: clean-extension clean-python-api clean-java
	cmake -E rm -rf build


# Utils
define config-cmake
	cmake -B build/$1 $(CMAKE_FLAGS) -DCMAKE_BUILD_TYPE=$2 $3 .
endef

define build-cmake
	cmake --build build/$1 --config $2
endef

define run-cmake
	$(call config-cmake,$1,$2,$3)
	$(call build-cmake,$1,$2)
endef

define run-cmake-debug
	$(call run-cmake,debug,Debug,$1)
endef

define build-cmake-release
	$(call build-cmake,release,Release,$1)
endef

define build-cmake-relwithdebinfo
	$(call build-cmake,relwithdebinfo,RelWithDebInfo,$1)
endef

define config-cmake-release
	$(call config-cmake,release,Release,$1)
endef

define config-cmake-relwithdebinfo
	$(call config-cmake,relwithdebinfo,RelWithDebInfo,$1)
endef

define run-cmake-release
	$(call config-cmake-release,$1)
	$(call build-cmake-release,$1)
endef

define run-cmake-relwithdebinfo
	$(call config-cmake-relwithdebinfo,$1)
	$(call build-cmake-relwithdebinfo,$1)
endef
