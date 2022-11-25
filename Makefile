.PHONY: all release debug test clean

all: release

GENERATOR=
FORCE_COLOR=
NUM_THREADS=
SANITIZER_FLAG=
ROOT_DIR=$(PWD)

ifndef $(NUM_THREADS)
	NUM_THREADS=1
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

release:
	mkdir -p build/release && \
	cd build/release && \
	cmake $(GENERATOR) $(FORCE_COLOR) $(SANITIZER_FLAG) -DCMAKE_BUILD_TYPE=Release ../.. && \
	cmake --build . --config Release -- -j $(NUM_THREADS)

debug:
	mkdir -p build/debug && \
	cd build/debug && \
	cmake $(GENERATOR) $(FORCE_COLOR) $(SANITIZER_FLAG) -DCMAKE_BUILD_TYPE=Debug ../.. && \
	cmake --build . --config Debug -- -j $(NUM_THREADS)


test:
	cd $(ROOT_DIR)/build/release/test && \
	ctest && \
	cd $(ROOT_DIR)/tools/python_api/test && \
	pytest

clean:
	rm -rf build
