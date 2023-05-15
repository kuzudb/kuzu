.PHONY: release debug test benchmark all alldebug arrow clean clean-external clean-all

GENERATOR=
FORCE_COLOR=
NUM_THREADS=
SANITIZER_FLAG=
ROOT_DIR=$(CURDIR)

ifndef $(NUM_THREADS)
	NUM_THREADS=1
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

arrow:
ifeq ($(OS),Windows_NT)
	cd external\ && \
	(if not exist build\ mkdir build\) && \
	cd build\ && \
	cmake $(FORCE_COLOR) $(SANITIZER_FLAG) $(GENERATOR) -DCMAKE_BUILD_TYPE=Release .. && \
	cmake --build . --config Release -- -j $(NUM_THREADS)
else
	cd external && \
	mkdir -p build && \
	cd build && \
	cmake $(GENERATOR) $(FORCE_COLOR) $(SANITIZER_FLAG) -DCMAKE_BUILD_TYPE=Release ../ && \
	cmake --build . --config Release -- -j $(NUM_THREADS)
endif

release: arrow
ifeq ($(OS),Windows_NT)
	(if not exist build\ mkdir build\) && \
	cd build\ && \
	(if not exist release\ mkdir release\) && \
	cd release\ &&\
	cmake $(GENERATOR) $(FORCE_COLOR) $(SANITIZER_FLAG) -DCMAKE_BUILD_TYPE=Release ..\.. && \
	cmake --build . --config Release -- -j $(NUM_THREADS)
else
	mkdir -p build/release && \
	cd build/release && \
	cmake $(GENERATOR) $(FORCE_COLOR) $(SANITIZER_FLAG) -DCMAKE_BUILD_TYPE=Release ../.. && \
	cmake --build . --config Release -- -j $(NUM_THREADS)
endif

debug: arrow
ifeq ($(OS),Windows_NT)
	(if not exist build\ mkdir build\) && \
	cd build\ && \
	(if not exist debug\ mkdir debug\) && \
	cd release\ &&\
	cmake $(GENERATOR) $(FORCE_COLOR) $(SANITIZER_FLAG) -DCMAKE_BUILD_TYPE=RelWithDebInfo ..\.. && \
	cmake --build . --config Debug -- -j $(NUM_THREADS)
else
	mkdir -p build/debug && \
	cd build/debug && \
	cmake $(GENERATOR) $(FORCE_COLOR) $(SANITIZER_FLAG) -DCMAKE_BUILD_TYPE=Debug ../.. && \
	cmake --build . --config Debug -- -j $(NUM_THREADS)
endif

all: arrow
	mkdir -p build/release && \
	cd build/release && \
	cmake $(GENERATOR) $(FORCE_COLOR) $(SANITIZER_FLAG) -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTS=TRUE -DBUILD_BENCHMARK=TRUE ../.. && \
	cmake --build . --config Release -- -j $(NUM_THREADS)

alldebug: arrow
	mkdir -p build/debug && \
	cd build/debug && \
	cmake $(GENERATOR) $(FORCE_COLOR) $(SANITIZER_FLAG) -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTS=TRUE -DBUILD_BENCHMARK=TRUE ../.. && \
	cmake --build . --config Debug -- -j $(NUM_THREADS)

benchmark: arrow
	mkdir -p build/release && \
	cd build/release && \
	cmake $(GENERATOR) $(FORCE_COLOR) $(SANITIZER_FLAG) -DCMAKE_BUILD_TYPE=Release -DBUILD_BENCHMARK=TRUE ../.. && \
	cmake --build . --config Release -- -j $(NUM_THREADS)

test: arrow
ifeq ($(OS),Windows_NT)
	(if not exist build\ mkdir build\) && \
	cd build\ && \
	(if not exist release\ mkdir release\) && \
	cd release\ &&\
	cmake $(GENERATOR) $(FORCE_COLOR) $(SANITIZER_FLAG) -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTS=TRUE ..\.. && \
	cmake --build . --config Release -- -j $(NUM_THREADS)
	cd $(ROOT_DIR)/build/release/test && \
	ctest
else
	mkdir -p build/release && \
	cd build/release && \
	cmake $(GENERATOR) $(FORCE_COLOR) $(SANITIZER_FLAG) -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTS=TRUE ../.. && \
	cmake --build . --config Release -- -j $(NUM_THREADS)
	cd $(ROOT_DIR)/build/release/test && \
	ctest
endif

lcov: arrow
	mkdir -p build/release && \
	cd build/release && \
	cmake $(GENERATOR) $(FORCE_COLOR) $(SANITIZER_FLAG) -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTS=TRUE -DBUILD_LCOV=TRUE ../.. && \
	cmake --build . --config Release -- -j $(NUM_THREADS)
	cd $(ROOT_DIR)/build/release/test && \
	ctest

pytest: arrow
	$(MAKE) release
	cd $(ROOT_DIR)/tools/python_api/test && \
	python3 -m pytest -v test_main.py

clean-python-api:
	rm -rf tools/python_api/build

clean-external:
	rm -rf external/build

clean: clean-python-api
	rm -rf build

clean-all: clean-external clean
