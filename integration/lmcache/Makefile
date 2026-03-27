# Convenience wrapper around cmake
BUILD_DIR ?= build
CMAKE_FLAGS ?=

.PHONY: all clean test install

all:
	@mkdir -p $(BUILD_DIR)
	cd $(BUILD_DIR) && cmake .. $(CMAKE_FLAGS) && make -j$$(nproc)

test:
	@mkdir -p $(BUILD_DIR)
	cd $(BUILD_DIR) && cmake .. -DBUILD_TESTS=ON $(CMAKE_FLAGS) && make -j$$(nproc)
	cd $(BUILD_DIR) && ctest --output-on-failure

clean:
	rm -rf $(BUILD_DIR)

install: all
	cd $(BUILD_DIR) && make install
