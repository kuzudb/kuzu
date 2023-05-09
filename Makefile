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

arrow:
	$(call mkdirp,external/build) && cd external/build && \
	cmake $(FORCE_COLOR) $(SANITIZER_FLAG) $(GENERATOR) -DCMAKE_BUILD_TYPE=Release .. && \
	cmake --build . --config Release -- -j $(NUM_THREADS)

release: arrow
	$(call mkdirp,build/release) && cd build/release && \
	cmake $(GENERATOR) $(FORCE_COLOR) $(SANITIZER_FLAG) -DCMAKE_BUILD_TYPE=Release ../.. && \
	cmake --build . --config Release -- -j $(NUM_THREADS)

debug: arrow
	$(call mkdirp,build/debug) && cd build/debug && \
	cmake $(GENERATOR) $(FORCE_COLOR) $(SANITIZER_FLAG) -DCMAKE_BUILD_TYPE=Debug ../.. && \
	cmake --build . --config Debug -- -j $(NUM_THREADS)

all: arrow
	$(call mkdirp,build/release) && cd build/release && \
	cmake $(GENERATOR) $(FORCE_COLOR) $(SANITIZER_FLAG) -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTS=TRUE -DBUILD_BENCHMARK=TRUE -DBUILD_NODEJS=TRUE ../.. && \
	cmake --build . --config Release -- -j $(NUM_THREADS)

alldebug: arrow
	$(call mkdirp,build/debug) && cd build/debug && \
	cmake $(GENERATOR) $(FORCE_COLOR) $(SANITIZER_FLAG) -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTS=TRUE -DBUILD_BENCHMARK=TRUE -DBUILD_NODEJS=TRUE ../.. && \
	cmake --build . --config Debug -- -j $(NUM_THREADS)

benchmark: arrow
	$(call mkdirp,build/release) && cd build/release && \
	cmake $(GENERATOR) $(FORCE_COLOR) $(SANITIZER_FLAG) -DCMAKE_BUILD_TYPE=Release -DBUILD_BENCHMARK=TRUE ../.. && \
	cmake --build . --config Release -- -j $(NUM_THREADS)

nodejs: arrow
	$(call mkdirp,build/release) && cd build/release && \
	cmake $(GENERATOR) $(FORCE_COLOR) $(SANITIZER_FLAG) -DCMAKE_BUILD_TYPE=Release -DBUILD_NODEJS=TRUE ../.. && \
	cmake --build . --config Release -- -j $(NUM_THREADS)

test: arrow
	$(call mkdirp,build/release) && cd build/release && \
	cmake $(GENERATOR) $(FORCE_COLOR) $(SANITIZER_FLAG) -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTS=TRUE ../.. && \
	cmake --build . --config Release -- -j $(NUM_THREADS)
	cd $(ROOT_DIR)/build/release/test && \
	ctest --output-on-failure -j ${TEST_JOBS}

lcov: arrow
	$(call mkdirp,build/release) && cd build/release && \
	cmake $(GENERATOR) $(FORCE_COLOR) $(SANITIZER_FLAG) -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTS=TRUE -DBUILD_NODEJS=TRUE -DBUILD_LCOV=TRUE ../.. && \
	cmake --build . --config Release -- -j $(NUM_THREADS)
	cd $(ROOT_DIR)/build/release/test && \
	ctest --output-on-failure -j ${TEST_JOBS}

pytest: arrow
	$(MAKE) release
	cd $(ROOT_DIR)/tools/python_api/test && \
	python3 -m pytest -v test_main.py

nodejstest: arrow
	$(MAKE) nodejs
	cd $(ROOT_DIR)/tools/nodejs_api/ && \
	npm test

rusttest:
ifeq ($(OS),Windows_NT)
	cd $(ROOT_DIR)/tools/rust_api && \
	set KUZU_TESTING=1 && \
	cargo test -- --test-threads=1
else
	cd $(ROOT_DIR)/tools/rust_api && \
	KUZU_TESTING=1 cargo test -- --test-threads=1
endif

clean-python-api:
ifeq ($(OS),Windows_NT)
	if exist tools\python_api\build rmdir /s /q tools\python_api\build
else
	rm -rf tools/python_api/build
endif

clean-external:
ifeq ($(OS),Windows_NT)
	if exist external\build rmdir /s /q external\build
else
	rm -rf external/build
endif

clean: clean-python-api
ifeq ($(OS),Windows_NT)
	if exist build rmdir /s /q build
else
	rm -rf build
endif

clean-all: clean-external clean
