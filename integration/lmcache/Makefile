# One-shot build for the dingofs-connector wheel.
#
# Usage:
#   make           # configure (if needed) -> build .so -> strip -> wheel
#   make native    # only build _dingofs_native.so
#   make strip     # strip debug info from the .so (in place)
#   make wheel     # build the wheel into ./dist/
#   make test      # ping + roundtrip against REMOTE_URL=dingofs://...
#   make clean     # remove built .so and ./dist/
#
# Overrides:
#   make JOBS=8 BUILD_TYPE=Release
#   make test REMOTE_URL=dingofs://mds:6700/grp HASH=0xcafebabe

SHELL := /bin/bash

DINGOFS_ROOT := $(abspath $(CURDIR)/../..)
BUILD_DIR    := $(DINGOFS_ROOT)/build
PKG_DIR      := $(CURDIR)/src/dingofs_connector
DIST_DIR     := $(CURDIR)/dist
JOBS         ?= $(shell nproc)
BUILD_TYPE   ?= RelWithDebInfo
PYTHON       ?= python

# Smoke test / bench config (set REMOTE_URL to enable).
REMOTE_URL  ?=
HASH        ?= 0xcafebabe
BENCH_OP    ?= get
BENCH_COUNT ?= 20
BENCH_SIZE  ?= $(shell echo $$((4*1024*1024)))
BENCH_DUR   ?= 10
BENCH_CONC  ?= 1

.PHONY: all wheel strip native configure ensure-flag test bench clean help

all: wheel

help:
	@echo "make           configure + build .so + strip + wheel (default)"
	@echo "make native    only build _dingofs_native.so"
	@echo "make strip     strip --strip-debug the .so in place"
	@echo "make wheel     produce dist/*.whl"
	@echo "make test      ping + roundtrip against REMOTE_URL"
	@echo "               e.g. make test REMOTE_URL=dingofs://mds:6700/grp"
	@echo "make bench     watch-mode bench against REMOTE_URL (default: get x 20 x 4MiB x 10s)"
	@echo "               override: BENCH_OP=put BENCH_COUNT=10 BENCH_SIZE=65536 BENCH_DUR=30 BENCH_CONC=4"
	@echo "make clean     remove .so and dist/"

# First-time configure: run cmake with the connector flag turned on.
configure: $(BUILD_DIR)/CMakeCache.txt

$(BUILD_DIR)/CMakeCache.txt:
	@mkdir -p $(BUILD_DIR)
	cd $(BUILD_DIR) && cmake \
	    -DCMAKE_BUILD_TYPE=$(BUILD_TYPE) \
	    -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
	    -DBUILD_UNIT_TESTS=OFF \
	    -DBUILD_LMCACHE_CONNECTOR=ON \
	    $(DINGOFS_ROOT)

# If the build dir was previously configured without the connector flag,
# flip it on without touching other settings (avoids a full reconfigure).
ensure-flag: configure
	@if ! grep -q '^BUILD_LMCACHE_CONNECTOR:BOOL=ON' $(BUILD_DIR)/CMakeCache.txt; then \
	    echo ">>> enabling BUILD_LMCACHE_CONNECTOR in existing build dir"; \
	    cd $(BUILD_DIR) && cmake -DBUILD_LMCACHE_CONNECTOR=ON . ; \
	fi

native: ensure-flag
	$(MAKE) -C $(BUILD_DIR) -j$(JOBS) _dingofs_native

strip: native
	@for so in $(PKG_DIR)/_dingofs_native*.so; do \
	    [ -f "$$so" ] || { echo "no .so found in $(PKG_DIR)" >&2; exit 1; }; \
	    echo "strip --strip-debug $$so"; \
	    strip --strip-debug "$$so"; \
	done

wheel: strip
	rm -rf $(DIST_DIR)
	$(PYTHON) -m pip wheel . --no-deps --no-build-isolation -w $(DIST_DIR)
	@echo
	@echo "wheel ready:"
	@ls -lh $(DIST_DIR)/*.whl

# Smoke test against a live cluster. Pass the cluster URL via REMOTE_URL
# (env var or `make test REMOTE_URL=...`). HASH defaults to 0xcafebabe.
test: native
	@if [ -z "$(REMOTE_URL)" ]; then \
	    echo "REMOTE_URL is required, e.g.:" >&2; \
	    echo "    make test REMOTE_URL=dingofs://mds1:6700,mds2:6700/lmcache_group" >&2; \
	    exit 2; \
	fi
	@echo ">>> ping $(REMOTE_URL)"
	$(PYTHON) examples/client.py --url '$(REMOTE_URL)' ping
	@echo ">>> roundtrip hash=$(HASH)"
	$(PYTHON) examples/client.py --url '$(REMOTE_URL)' roundtrip --hash $(HASH)

# watch-mode benchmark. Default: get x 20 keys x 4 MiB x 10 s, concurrency=1.
# All knobs overridable: BENCH_OP / BENCH_COUNT / BENCH_SIZE / BENCH_DUR / BENCH_CONC.
bench: native
	@if [ -z "$(REMOTE_URL)" ]; then \
	    echo "REMOTE_URL is required, e.g.:" >&2; \
	    echo "    make bench REMOTE_URL=dingofs://10.220.88.31:6900/group-1" >&2; \
	    exit 2; \
	fi
	@echo ">>> bench op=$(BENCH_OP) count=$(BENCH_COUNT) size=$(BENCH_SIZE) duration=$(BENCH_DUR)s concurrency=$(BENCH_CONC)"
	$(PYTHON) examples/client.py --url '$(REMOTE_URL)' watch \
	    --op $(BENCH_OP) \
	    --count $(BENCH_COUNT) \
	    --size $(BENCH_SIZE) \
	    --duration $(BENCH_DUR) \
	    --concurrency $(BENCH_CONC)

clean:
	rm -f $(PKG_DIR)/_dingofs_native*.so
	rm -rf $(DIST_DIR) $(CURDIR)/build
