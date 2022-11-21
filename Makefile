release:
	mkdir -p build/release && \
	cd build/release && \
	cmake $(GENERATOR) $(FORCE_COLOR) ${WARNINGS_AS_ERRORS} ${FORCE_WARN_UNUSED_FLAG} ${FORCE_32_BIT_FLAG} ${DISABLE_UNITY_FLAG} ${DISABLE_SANITIZER_FLAG} ${OSX_BUILD_UNIVERSAL_FLAG} ${STATIC_LIBCPP} ${EXTENSIONS} -DCMAKE_BUILD_TYPE=Release ../.. && \
	cmake --build . --config Release -- -j 8

debug:
	mkdir -p build/debug && \
	cd build/debug && \
	cmake $(GENERATOR) $(FORCE_COLOR) ${WARNINGS_AS_ERRORS} ${FORCE_32_BIT_FLAG} ${DISABLE_UNITY_FLAG} ${DISABLE_SANITIZER_FLAG} ${STATIC_LIBCPP} ${EXTENSIONS} -DCMAKE_BUILD_TYPE=Debug ../.. && \
	cmake --build . --config Debug -- -j 8

clean:
	rm -rf build
