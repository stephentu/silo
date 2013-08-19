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
else ifeq ($(strip $(MODE)),factor-gc)
	O = out-factor-gc$(OSUFFIX)
	CONFIG_H = config/config-factor-gc.h
else ifeq ($(strip $(MODE)),factor-gc-nowriteinplace)
	O = out-factor-gc-nowriteinplace$(OSUFFIX)
	CONFIG_H = config/config-factor-gc-nowriteinplace.h
endif

ifeq ($(strip $(DEBUG)),1)
        CXXFLAGS := -Ithird-party/lz4 -Wall -g -fno-omit-frame-pointer --std=c++0x -DCONFIG_H=\"$(CONFIG_H)\"
else
        CXXFLAGS := -Ithird-party/lz4 -Wall -g -Werror -O2 -funroll-loops -fno-omit-frame-pointer --std=c++0x -DCONFIG_H=\"$(CONFIG_H)\"
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

# XXX(stephentu): have GCC discover header deps for us automatically
HEADERS = allocator.h \
	amd64.h \
	base_txn_btree.h \
	btree.h \
	btree_impl.h \
	circbuf.h \
	core.h \
	counter.h \
	fileutils.h \
	imstring.h \
	keyrange.h \
	lockguard.h \
	macros.h \
	marked_ptr.h \
	ndb_type_traits.h \
	prefetch.h \
	rcu.h \
	record/cursor.h \
	record/encoder.h \
	record/inline_str.h \
	record/serializer.h \
	scopedperf.hh \
	small_unordered_map.h \
	small_vector.h \
	spinbarrier.h \
	spinlock.h \
	static_unordered_map.h \
	static_vector.h \
	stats_common.h \
	stats_server.h \
	ticker.h \
	thread.h \
	tuple.h \
	txn_btree.h \
	txn.h \
	txn_impl.h \
	txn_proto1_impl.h \
	txn_proto2_impl.h \
	typed_txn_btree.h \
	util.h \
	varint.h \
	varkey.h \
	$(CONFIG_H)
SRCFILES = allocator.cc \
	btree.cc \
	core.cc \
	counter.cc \
	keyrange.cc \
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

BENCH_CXXFLAGS := $(CXXFLAGS) -DMYSQL_SHARE_DIR=\"$(MYSQL_SHARE_DIR)\"
BENCH_LDFLAGS := $(LDFLAGS) -L/usr/lib/mysql -ldb_cxx -lmysqld -lz -lrt -lcrypt -laio -ldl -lssl -lcrypto

BENCH_HEADERS = $(HEADERS) \
	benchmarks/abstract_db.h \
	benchmarks/abstract_ordered_index.h \
	benchmarks/bdb_wrapper.h \
	benchmarks/bench.h \
	benchmarks/kvdb_wrapper.h \
	benchmarks/kvdb_wrapper_impl.h \
	benchmarks/masstree/kvrandom.hh \
	benchmarks/mysql_wrapper.h \
	benchmarks/ndb_wrapper.h \
	benchmarks/ndb_wrapper_impl.h \
	benchmarks/str_arena.h \
	benchmarks/tpcc.h
BENCH_SRCFILES = benchmarks/bdb_wrapper.cc \
	benchmarks/bench.cc \
	benchmarks/encstress.cc \
	benchmarks/masstree/kvrandom.cc \
	benchmarks/mysql_wrapper.cc \
	benchmarks/queue.cc \
	benchmarks/tpcc.cc \
	benchmarks/ycsb.cc

BENCH_OBJFILES := $(patsubst %.cc, $(O)/%.o, $(BENCH_SRCFILES))

NEWBENCH_HEADERS = $(HEADERS) \
	new-benchmarks/abstract_db.h \
	new-benchmarks/abstract_ordered_index.h \
	new-benchmarks/bench.h \
	new-benchmarks/kvdb_database.h \
	new-benchmarks/ndb_database.h \
	new-benchmarks/str_arena.h \
	new-benchmarks/tpcc.h
NEWBENCH_SRCFILES = new-benchmarks/bench.cc \
	new-benchmarks/tpcc.cc

NEWBENCH_OBJFILES := $(patsubst %.cc, $(O)/%.o, $(NEWBENCH_SRCFILES))

all: $(O)/test

$(O)/benchmarks/%.o: benchmarks/%.cc $(BENCH_HEADERS)
	@mkdir -p $(@D)
	$(CXX) $(BENCH_CXXFLAGS) -c $< -o $@

$(O)/benchmarks/masstree/%.o: benchmarks/masstree/%.cc $(BENCH_HEADERS)
	@mkdir -p $(@D)
	$(CXX) $(BENCH_CXXFLAGS) -c $< -o $@

$(O)/new-benchmarks/%.o: new-benchmarks/%.cc $(NEWBENCH_HEADERS)
	@mkdir -p $(@D)
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(O)/%.o: %.cc $(HEADERS)
	@mkdir -p $(@D)
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(O)/test: $(O)/test.o $(OBJFILES) third-party/lz4/liblz4.so
	$(CXX) -o $(O)/test $^ $(LDFLAGS) $(LZ4LDFLAGS)

third-party/lz4/liblz4.so:
	make -C third-party/lz4 library

$(O)/persist_test: persist_test.o third-party/lz4/liblz4.so
	$(CXX) -o $(O)/persist_test persist_test.o $(LDFLAGS) $(LZ4LDFLAGS)

$(O)/stats_client: stats_client.o $(HEADERS)
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

.PHONY: clean
clean:
	rm -rf out-check-invariants out-perf out-factor-gc
	make -C third-party/lz4 clean
