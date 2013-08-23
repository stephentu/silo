-include config.mk

### Options ###

DEBUG ?= 0

# 0 = libc malloc
# 1 = jemalloc
# 2 = tcmalloc
# 3 = flow
USE_MALLOC_MODE ?= 1

MYSQL_SHARE_DIR ?= /x/stephentu/mysql-5.5.29/build/sql/share

###############

# Available modes
#   * check-invariants
#   * perf
#   * factor-gc
#   * factor-gc-nowriteinplace
#   * sandbox
MODE ?= perf

ifeq ($(strip $(DEBUG)),1)
	OSUFFIX=.debug
else
	OSUFFIX=
endif

ifeq ($(strip $(MODE)),check-invariants)
	O = out-check-invariants$(OSUFFIX)
	CONFIG_H = config/config-check-invariants.h
else ifeq ($(strip $(MODE)),perf)
	O = out-perf$(OSUFFIX)
	CONFIG_H = config/config-perf.h
else ifeq ($(strip $(MODE)),perf-counters)
	O = out-perf-counters$(OSUFFIX)
	CONFIG_H = config/config-perf-counters.h
else ifeq ($(strip $(MODE)),factor-gc)
	O = out-factor-gc$(OSUFFIX)
	CONFIG_H = config/config-factor-gc.h
else ifeq ($(strip $(MODE)),factor-gc-nowriteinplace)
	O = out-factor-gc-nowriteinplace$(OSUFFIX)
	CONFIG_H = config/config-factor-gc-nowriteinplace.h
else ifeq ($(strip $(MODE)),sandbox)
	O = out-sandbox$(OSUFFIX)
	CONFIG_H = config/config-sandbox.h
else
	$(error invalid mode)
endif

ifeq ($(strip $(DEBUG)),1)
        CXXFLAGS := -MD -Ithird-party/lz4 -Wall -g -fno-omit-frame-pointer --std=c++0x -DCONFIG_H=\"$(CONFIG_H)\"
else
        CXXFLAGS := -MD -Ithird-party/lz4 -Wall -g -Werror -O2 -funroll-loops -fno-omit-frame-pointer --std=c++0x -DCONFIG_H=\"$(CONFIG_H)\"
endif

TOP     := $(shell echo $${PWD-`pwd`})
LDFLAGS := -lpthread -lnuma -lrt

LZ4LDFLAGS := -Lthird-party/lz4 -llz4 -Wl,-rpath,$(TOP)/third-party/lz4

ifeq ($(strip $(USE_MALLOC_MODE)),1)
        CXXFLAGS+=-DUSE_JEMALLOC
        LDFLAGS+=-ljemalloc
else ifeq ($(strip $(USE_MALLOC_MODE)),2)
        CXXFLAGS+=-DUSE_TCMALLOC
        LDFLAGS+=-ltcmalloc
else ifeq ($(strip $(USE_MALLOC_MODE)),3)
        CXXFLAGS+=-DUSE_FLOW
        LDFLAGS+=-lflow
endif

ifneq ($(strip $(CUSTOM_LDPATH)), )
        LDFLAGS+=$(CUSTOM_LDPATH)
endif

SRCFILES = allocator.cc \
	btree.cc \
	core.cc \
	counter.cc \
	memory.cc \
	rcu.cc \
	stats_server.cc \
	thread.cc \
	ticker.cc \
	tuple.cc \
	txn_btree.cc \
	txn.cc \
	txn_proto1_impl.cc \
	txn_proto2_impl.cc \
	varint.cc

OBJFILES := $(patsubst %.cc, $(O)/%.o, $(SRCFILES))
DEPFILES := $(patsubst %.cc, $(O)/%.d, $(SRCFILES))

BENCH_CXXFLAGS := $(CXXFLAGS) -DMYSQL_SHARE_DIR=\"$(MYSQL_SHARE_DIR)\"
BENCH_LDFLAGS := $(LDFLAGS) -L/usr/lib/mysql -ldb_cxx -lmysqld -lz -lrt -lcrypt -laio -ldl -lssl -lcrypto

BENCH_SRCFILES = benchmarks/bdb_wrapper.cc \
	benchmarks/bench.cc \
	benchmarks/encstress.cc \
	benchmarks/masstree/kvrandom.cc \
	benchmarks/mysql_wrapper.cc \
	benchmarks/queue.cc \
	benchmarks/tpcc.cc \
	benchmarks/ycsb.cc

BENCH_OBJFILES := $(patsubst %.cc, $(O)/%.o, $(BENCH_SRCFILES))
BENCH_DEPFILES := $(patsubst %.cc, $(O)/%.d, $(BENCH_SRCFILES))

NEWBENCH_SRCFILES = new-benchmarks/bench.cc \
	new-benchmarks/tpcc.cc

NEWBENCH_OBJFILES := $(patsubst %.cc, $(O)/%.o, $(NEWBENCH_SRCFILES))
NEWBENCH_DEPFILES := $(patsubst %.cc, $(O)/%.d, $(NEWBENCH_SRCFILES))

all: $(O)/test

$(O)/benchmarks/%.o: benchmarks/%.cc
	@mkdir -p $(@D)
	$(CXX) $(BENCH_CXXFLAGS) -c $< -o $@

$(O)/benchmarks/masstree/%.o: benchmarks/masstree/%.cc
	@mkdir -p $(@D)
	$(CXX) $(BENCH_CXXFLAGS) -c $< -o $@

$(O)/new-benchmarks/%.o: new-benchmarks/%.cc
	@mkdir -p $(@D)
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(O)/%.o: %.cc
	@mkdir -p $(@D)
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(O)/test: $(O)/test.o $(OBJFILES) third-party/lz4/liblz4.so
	$(CXX) -o $(O)/test $^ $(LDFLAGS) $(LZ4LDFLAGS)

third-party/lz4/liblz4.so:
	make -C third-party/lz4 library

$(O)/persist_test: persist_test.o third-party/lz4/liblz4.so
	$(CXX) -o $(O)/persist_test persist_test.o $(LDFLAGS) $(LZ4LDFLAGS)

$(O)/stats_client: stats_client.o
	$(CXX) -o $(O)/stats_client stats_client.o $(LDFLAGS)

.PHONY: dbtest
dbtest: $(O)/benchmarks/dbtest

$(O)/benchmarks/dbtest: $(O)/benchmarks/dbtest.o $(OBJFILES) $(BENCH_OBJFILES) third-party/lz4/liblz4.so
	$(CXX) -o $(O)/benchmarks/dbtest $^ $(BENCH_LDFLAGS) $(LZ4LDFLAGS)

.PHONY: kvtest
kvtest: $(O)/benchmarks/masstree/kvtest

$(O)/benchmarks/masstree/kvtest: $(O)/benchmarks/masstree/kvtest.o $(OBJFILES) $(BENCH_OBJFILES)
	$(CXX) -o $(O)/benchmarks/masstree/kvtest $^ $(BENCH_LDFLAGS)

.PHONY: newdbtest
newdbtest: $(O)/new-benchmarks/dbtest

$(O)/new-benchmarks/dbtest: $(O)/new-benchmarks/dbtest.o $(OBJFILES) $(NEWBENCH_OBJFILES) third-party/lz4/liblz4.so
	$(CXX) -o $(O)/new-benchmarks/dbtest $^ $(LDFLAGS) $(LZ4LDFLAGS)

-include $(DEPFILES)
-include $(BENCH_DEPFILES)
-include $(NEWBENCH_DEPFILES)

# executables
-include $(O)/test.d
-include $(O)/persist_test.d
-include $(O)/stats_client.d
-include $(O)/benchmarks/dbtest.d
-include $(O)/benchmarks/masstress/kvtest.d
-include $(O)/new-benchmarks/dbtest.d

.PHONY: clean
clean:
	rm -rf out-*
	make -C third-party/lz4 clean
