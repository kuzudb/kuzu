# Helper frontend to cmake.
# Tip: to see the actual commands that will be run for any target, use `make -n <target>`.

.DEFAULT_GOAL := release
# Explicit targets to avoid conflict with files of the same name.
.PHONY: \
	release relwithdebinfo debug all allconfig alldebug \
	test-build test lcov \
	java_native_header java javatest \
	nodejs nodejstest \
	python python-debug pytest pytest-debug \
	wasm wasmtest \
	rusttest \
	benchmark example \
	extension-test-build extension-test extension-json-test-build extension-json-test \
	extension-debug extension-release \
	shell-test \
	tidy tidy-analyzer clangd-diagnostics \
	install \
	clean-extension clean-python-api clean-java clean
.ONESHELL:
.SHELLFLAGS = -ec

BUILD_TYPE ?=
BUILD_PATH ?=
EXTRA_CMAKE_FLAGS ?=
CLANGD_DIAGNOSTIC_INSTANCES ?= 4
PREFIX ?= install
TEST_JOBS ?= 10
EXTENSION_LIST ?= httpfs;duckdb;json;postgres;sqlite;fts;delta;iceberg;azure;unity_catalog;vector;neo4j;algo;llm
EXTENSION_TEST_EXCLUDE_FILTER ?= ""

ifeq ($(shell uname -s 2>/dev/null),Linux)
	NUM_THREADS ?= $(shell expr $(shell nproc) \* 2 / 3)
else ifeq ($(shell uname -s 2>/dev/null),Darwin)
	NUM_THREADS ?= $(shell expr $(shell sysctl -n hw.ncpu) \* 2 / 3)
else
	NUM_THREADS ?= 1
endif
export CMAKE_BUILD_PARALLEL_LEVEL=$(NUM_THREADS)

ifeq ($(OS),Windows_NT)
	GEN ?= Ninja
	SHELL := cmd.exe
	.SHELLFLAGS := /c
endif

ifdef GEN
	CMAKE_FLAGS += -G "$(GEN)"
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

ifdef ENABLE_DESER_DEBUG
	CMAKE_FLAGS += -DENABLE_DESER_DEBUG=$(ENABLE_DESER_DEBUG)
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

ifdef PAGE_SIZE_LOG2
	CMAKE_FLAGS += -DKUZU_PAGE_SIZE_LOG2=$(PAGE_SIZE_LOG2)
endif

ifdef DEFAULT_REL_STORAGE_DIRECTION
	CMAKE_FLAGS += -DKUZU_DEFAULT_REL_STORAGE_DIRECTION=$(DEFAULT_REL_STORAGE_DIRECTION)
endif

ifdef VECTOR_CAPACITY_LOG2
	CMAKE_FLAGS += -DKUZU_VECTOR_CAPACITY_LOG2=$(VECTOR_CAPACITY_LOG2)
endif

ifdef NODE_GROUP_SIZE_LOG2
	CMAKE_FLAGS += -DKUZU_NODE_GROUP_SIZE_LOG2=$(NODE_GROUP_SIZE_LOG2)
endif

ifdef SINGLE_THREADED
	CMAKE_FLAGS += -DSINGLE_THREADED=$(SINGLE_THREADED)
endif

ifdef WASM_NODEFS
	CMAKE_FLAGS += -DWASM_NODEFS=$(WASM_NODEFS)
endif

ifdef USE_STD_FORMAT
	CMAKE_FLAGS += -DUSE_STD_FORMAT=$(USE_STD_FORMAT)
endif

ifdef EXTRA_CMAKE_FLAGS
	CMAKE_FLAGS += $(EXTRA_CMAKE_FLAGS)
endif

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
		-DBUILD_EXTENSIONS="$(EXTENSION_LIST)" \
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
		-DBUILD_EXTENSIONS="$(EXTENSION_LIST)" \
		-DBUILD_JAVA=TRUE \
		-DBUILD_NODEJS=TRUE \
		-DBUILD_PYTHON=TRUE \
		-DBUILD_SHELL=TRUE \
		-DBUILD_TESTS=TRUE \
	)

# Main tests
test-build:
	$(call run-cmake-relwithdebinfo, -DBUILD_TESTS=TRUE -DENABLE_BACKTRACES=TRUE)

test: test-build
	python3 dataset/ldbc-1/download_data.py
	ctest --test-dir build/$(call get-build-path,RelWithDebInfo)/test --output-on-failure -j ${TEST_JOBS}

lcov:
	python3 dataset/ldbc-1/download_data.py
	$(call run-cmake-release, -DBUILD_TESTS=TRUE -DBUILD_LCOV=TRUE)
	ctest --test-dir build/$(call get-build-path,Release)/test --output-on-failure -j ${TEST_JOBS}

# Language APIs

# Required for clangd-related tools.
java_native_header:
	cmake --build build/$(call get-build-path,Release) --target kuzu_java

java:
	$(call run-cmake-release, -DBUILD_JAVA=TRUE)

javatest:
ifeq ($(OS),Windows_NT)
	cd tools/java_api &&\
	gradlew.bat test -i
else
	cd tools/java_api &&\
	./gradlew test -i
endif

nodejs:
	$(call run-cmake-release, -DBUILD_NODEJS=TRUE)

nodejs-deps:
	cd tools/nodejs_api && npm install --include=dev

nodejstest: nodejs
	cd tools/nodejs_api && npm test

nodejstest-deps: nodejs-deps nodejstest

python:
	$(call run-cmake-release, -DBUILD_PYTHON=TRUE -DBUILD_SHELL=FALSE)

python-debug:
	$(call run-cmake-debug, -DBUILD_PYTHON=TRUE)

pytest: python
	cmake -E env PYTHONPATH=tools/python_api/build python3 -m pytest -vv tools/python_api/test

pytest-venv: python
	$(MAKE) -C tools/python_api pytest

pytest-debug: python-debug
	cmake -E env PYTHONPATH=tools/python_api/build python3 -m pytest -vv tools/python_api/test

wasm:
	mkdir -p build/wasm && cd build/wasm &&\
	emcmake cmake $(CMAKE_FLAGS) -DCMAKE_BUILD_TYPE=$(call get-build-type,Release) -DBUILD_WASM=TRUE -DBUILD_BENCHMARK=FALSE -DBUILD_TESTS=FALSE -DBUILD_SHELL=FALSE  ../.. && \
	cmake --build . --config $(call get-build-type,Release) -j $(NUM_THREADS)

wasmtest:
	mkdir -p build/wasm && cd build/wasm &&\
	emcmake cmake $(CMAKE_FLAGS) -DCMAKE_BUILD_TYPE=$(call get-build-type,Release) -DBUILD_WASM=TRUE -DBUILD_BENCHMARK=FALSE -DBUILD_TESTS=TRUE -DBUILD_SHELL=FALSE  ../.. && \
	cmake --build . --config $(call get-build-type,Release) -j $(NUM_THREADS) &&\
	cd ../.. && ctest --test-dir  build/wasm/test/ --output-on-failure -j ${TEST_JOBS} --timeout 600

rusttest:
ifeq ($(OS),Windows_NT)
	set CARGO_BUILD_JOBS=$(NUM_THREADS)
else
	export CARGO_BUILD_JOBS=$(NUM_THREADS)
endif
	cd tools/rust_api && cargo test --release --locked --all-features

# Other misc build targets
benchmark:
	$(call run-cmake-release, -DBUILD_BENCHMARK=TRUE)

example:
	$(call run-cmake-release, -DBUILD_EXAMPLES=TRUE)

extension-build:
	$(call run-cmake-relwithdebinfo,-DBUILD_EXTENSIONS="$(EXTENSION_LIST)")

extension-test-build:
	$(call run-cmake-relwithdebinfo, \
		-DBUILD_EXTENSIONS="$(EXTENSION_LIST)" \
		-DBUILD_EXTENSION_TESTS=TRUE \
		-DBUILD_TESTS=TRUE \
	)

extension-test: extension-test-build
	$(if $(filter Windows_NT,$(OS)),\
		set "E2E_TEST_FILES_DIRECTORY=extension" &&,\
		E2E_TEST_FILES_DIRECTORY=extension) \
    ctest --test-dir build/$(call get-build-path,RelWithDebInfo)/test/runner --output-on-failure -j ${TEST_JOBS} --exclude-regex "${EXTENSION_TEST_EXCLUDE_FILTER}"
	aws s3 rm s3://kuzu-dataset-us/${RUN_ID}/ --recursive

extension-json-test-build:
	$(call run-cmake-relwithdebinfo, \
		-DBUILD_EXTENSIONS="json" \
		-DBUILD_EXTENSION_TESTS=TRUE \
		-DENABLE_ADDRESS_SANITIZER=TRUE \
	)

extension-json-test: extension-json-test-build
	ctest --test-dir build/$(call get-build-path,RelWithDebInfo)/extension --output-on-failure -j ${TEST_JOBS} -R json
	aws s3 rm s3://kuzu-dataset-us/${RUN_ID}/ --recursive

extension-debug:
	$(call run-cmake-debug, \
		-DBUILD_EXTENSIONS="$(EXTENSION_LIST)" \
		-DBUILD_KUZU=FALSE \
	)

extension-release:
	$(call run-cmake-release, \
		-DBUILD_EXTENSIONS="$(EXTENSION_LIST)" \
		-DBUILD_KUZU=FALSE \
	)

# pytest expects a `Release` build path.
shell-test:
	$(call run-cmake-release, \
		-DBUILD_SHELL=TRUE \
	)
	cd tools/shell/test && python3 -m pytest -v

# Clang-related tools and checks

# Must build the java native header to avoid missing includes. Pipe character
# `|` ensures these targets build in this order, even in the presence of
# parallelism.
tidy: | allconfig java_native_header
	run-clang-tidy -p build/$(call get-build-path,Release) -quiet -j $(NUM_THREADS) \
		"^$(realpath src)|$(realpath extension)/(?!fts/third_party/snowball/)|$(realpath tools)/(?!shell/linenoise.cpp)"

tidy-analyzer: | allconfig java_native_header
	run-clang-tidy -config-file .clang-tidy-analyzer -p build/$(call get-build-path,Release) -quiet -j $(NUM_THREADS) \
		"^$(realpath src)/(?!function/vector_cast_functions.cpp)|$(realpath extension)/(?!fts/third_party/snowball/)|$(realpath tools)/(?!shell/linenoise.cpp)"

clangd-diagnostics: | allconfig java_native_header
	find src -name *.h -or -name *.cpp | xargs \
		./scripts/get-clangd-diagnostics.py --compile-commands-dir build/$(call get-build-path,Release) \
		-j $(NUM_THREADS) --instances $(CLANGD_DIAGNOSTIC_INSTANCES)


# Installation
install:
	cmake --install build/$(call get-build-path,Release) --prefix $(PREFIX)


# Cleaning
clean-extension:
	cmake -E rm -rf extension/*/build

clean-python-api:
	cmake -E rm -rf tools/python_api/build

clean-java:
	cmake -E rm -rf tools/java_api/build

clean: clean-extension clean-python-api clean-java
	cmake -E rm -rf build


# Utils
lowercase = $(if $(filter Release,$(1)),release,$(if $(filter RelWithDebInfo,$(1)),relwithdebinfo,$(if $(filter Debug,$(1)),debug,$(1))))
get-build-type = $(if $(BUILD_TYPE),$(BUILD_TYPE),$1)
get-build-path = $(if $(BUILD_PATH),$(BUILD_PATH),$(call lowercase,$(call get-build-type,$(1))))

define config-cmake
	cmake -B build/$(call get-build-path,$1) -DCMAKE_BUILD_TYPE=$(call get-build-type,$1) $2 $(CMAKE_FLAGS) $(EXTRA_CMAKE_FLAGS) .
endef

define build-cmake
	cmake --build build/$(call get-build-path,$1) --config $(call get-build-type,$1)
endef

define run-cmake
	$(call config-cmake,$1,$2)
	$(call build-cmake,$1)
endef

define run-cmake-debug
	$(call run-cmake,Debug,$1)
endef

define build-cmake-release
	$(call build-cmake,Release,$1)
endef

define build-cmake-relwithdebinfo
	$(call build-cmake,RelWithDebInfo,$1)
endef

define config-cmake-release
	$(call config-cmake,Release,$1)
endef

define config-cmake-relwithdebinfo
	$(call config-cmake,RelWithDebInfo,$1)
endef

define run-cmake-release
	$(call config-cmake-release,$1)
	$(call build-cmake-release,$1)
endef

define run-cmake-relwithdebinfo
	$(call config-cmake-relwithdebinfo,$1)
	$(call build-cmake-relwithdebinfo,$1)
endef
