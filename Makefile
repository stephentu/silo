-include config.mk

DEBUG ?= 0

ifeq ($(strip $(DEBUG)),1)
        CXXFLAGS := -Wall -g -fno-omit-frame-pointer --std=c++0x
else
        CXXFLAGS := -Wall -Werror -g -O2 -funroll-loops -fno-omit-frame-pointer --std=c++0x
endif

LDFLAGS := -lpthread -lnuma

# 0 = libc malloc
# 1 = jemalloc
# 2 = tcmalloc
# 3 = flow
USE_MALLOC_MODE ?= 1

# 0 = disable perf counters
# 1 = enable perf counters
USE_PERF_CTRS ?= 0

ifeq ($(strip $(USE_MALLOC_MODE)),1)
        CXXFLAGS+=-DUSE_JEMALLOC
        LDFLAGS+=-ljemalloc
else
ifeq ($(strip $(USE_MALLOC_MODE)),2)
        CXXFLAGS+=-DUSE_TCMALLOC
        LDFLAGS+=-ltcmalloc
else
ifeq ($(strip $(USE_MALLOC_MODE)),3)
        CXXFLAGS+=-DUSE_FLOW
        LDFLAGS+=-lflow
endif
endif
endif

ifneq ($(strip $(CUSTOM_LDPATH)), )
        LDFLAGS+=$(CUSTOM_LDPATH)
endif

ifeq ($(USE_PERF_CTRS),1)
	CXXFLAGS+=-DUSE_PERF_CTRS
endif

HEADERS = allocator.h \
	amd64.h \
	btree.h \
	core.h \
	counter.h \
	hash_bytes.h \
	imstring.h \
	keyrange.h \
	lockguard.h \
	macros.h \
	marked_ptr.h \
	ndb_type_traits.h \
	prefetch.h \
	rcu.h \
	record/encoder.h \
	record/serializer.h \
	scopedperf.hh \
	small_unordered_map.h \
	small_vector.h \
	spinbarrier.h \
	spinlock.h \
	static_assert.h \
	static_unordered_map.h \
	static_vector.h \
	thread.h \
	tuple.h \
	txn_btree.h \
	txn_btree_impl.h \
	txn.h \
	txn_impl.h \
	txn_proto1_impl.h \
	txn_proto2_impl.h \
	util.h \
	varint.h \
	varkey.h \
	xbuf.h
SRCFILES = allocator.cc \
	btree.cc \
	core.cc \
	counter.cc \
	hash_bytes.cc \
	keyrange.cc \
	memory.cc \
	rcu.cc \
	thread.cc \
	tuple.cc \
	txn_btree.cc \
	txn.cc \
	txn_proto1_impl.cc \
	txn_proto2_impl.cc \
	varint.cc

OBJFILES = $(SRCFILES:.cc=.o)

MYSQL_SHARE_DIR=/x/stephentu/mysql-5.5.29/build/sql/share

BENCH_CXXFLAGS := $(CXXFLAGS) -DMYSQL_SHARE_DIR=\"$(MYSQL_SHARE_DIR)\"
BENCH_LDFLAGS := $(LDFLAGS) -L/usr/lib/mysql -ldb_cxx -lmysqld -lz -lrt -lcrypt -laio -ldl -lssl -lcrypto

BENCH_HEADERS = $(HEADERS) \
	benchmarks/abstract_db.h \
	benchmarks/abstract_ordered_index.h \
	benchmarks/bdb_wrapper.h \
	benchmarks/bench.h \
	benchmarks/inline_str.h \
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
BENCH_OBJFILES = $(BENCH_SRCFILES:.cc=.o)

all: test

benchmarks/%.o: benchmarks/%.cc $(BENCH_HEADERS)
	$(CXX) $(BENCH_CXXFLAGS) -c $< -o $@

benchmarks/masstree/%.o: benchmarks/masstree/%.cc $(BENCH_HEADERS)
	$(CXX) $(BENCH_CXXFLAGS) -c $< -o $@

%.o: %.cc $(HEADERS)
	$(CXX) $(CXXFLAGS) -c $< -o $@

test: test.o $(OBJFILES)
	$(CXX) $(CXXFLAGS) -o test $^ $(LDFLAGS)

.PHONY: dbtest
dbtest: benchmarks/dbtest

benchmarks/dbtest: benchmarks/dbtest.o $(OBJFILES) $(BENCH_OBJFILES)
	$(CXX) $(BENCH_CXXFLAGS) -o benchmarks/dbtest $^ $(BENCH_LDFLAGS)

.PHONY: kvtest
kvtest: benchmarks/masstree/kvtest

benchmarks/masstree/kvtest: benchmarks/masstree/kvtest.o $(OBJFILES) $(BENCH_OBJFILES)
	$(CXX) $(BENCH_CXXFLAGS) -o benchmarks/masstree/kvtest $^ $(BENCH_LDFLAGS)

.PHONY: clean
clean:
	rm -f *.o test benchmarks/*.o benchmarks/dbtest benchmarks/masstree/*.o benchmarks/masstree/kvtest
