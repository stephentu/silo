-include config.mk
CXXFLAGS := -Wall -Werror -g -O2 -funroll-loops -fno-omit-frame-pointer --std=c++0x
#CXXFLAGS := -Wall -g

LDFLAGS := -lpthread

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
        #LDFLAGS+=-L$(HOME)/jemalloc-bin/lib -ljemalloc
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

ifeq ($(USE_PERF_CTRS),1)
	CXXFLAGS+=-DUSE_PERF_CTRS
endif

HEADERS = btree.h macros.h rcu.h static_assert.h thread.h txn.h txn_btree.h varkey.h util.h \
	  spinbarrier.h counter.h core.h imstring.h lockguard.h spinlock.h hash_bytes.h \
	  prefetch.h xbuf.h small_vector.h small_unordered_map.h scopedperf.hh \
	  record/encoder.h record/serializer.h
SRCFILES = btree.cc counter.cc core.cc rcu.cc thread.cc txn.cc \
	txn_btree.cc varint.cc memory.cc hash_bytes.cc
OBJFILES = $(SRCFILES:.cc=.o)

MYSQL_SHARE_DIR=/x/stephentu/mysql-5.5.29/build/sql/share

BENCH_CXXFLAGS := $(CXXFLAGS) -DMYSQL_SHARE_DIR=\"$(MYSQL_SHARE_DIR)\"
BENCH_LDFLAGS := $(LDFLAGS) -L/usr/lib/mysql -ldb_cxx -lmysqld -lz -lrt -lcrypt -laio -ldl -lssl -lcrypto

BENCH_HEADERS = $(HEADERS) \
	benchmarks/abstract_db.h \
	benchmarks/abstract_ordered_index.h \
	benchmarks/bench.h \
	benchmarks/inline_str.h \
	benchmarks/bdb_wrapper.h \
	benchmarks/ndb_wrapper.h \
	benchmarks/mysql_wrapper.h \
	benchmarks/tpcc.h \
	benchmarks/masstree/kvrandom.hh
BENCH_SRCFILES = \
	benchmarks/bdb_wrapper.cc \
	benchmarks/ndb_wrapper.cc \
	benchmarks/mysql_wrapper.cc \
	benchmarks/tpcc.cc \
	benchmarks/ycsb.cc \
	benchmarks/queue.cc \
	benchmarks/encstress.cc \
	benchmarks/bench.cc \
	benchmarks/masstree/kvrandom.cc
BENCH_OBJFILES = $(BENCH_SRCFILES:.cc=.o)

all: test

benchmarks/%.o: benchmarks/%.cc $(BENCH_HEADERS)
	$(CXX) $(BENCH_CXXFLAGS) -c $< -o $@

benchmarks/masstree/%.o: benchmarks/masstree/%.cc $(BENCH_HEADERS)
	$(CXX) $(BENCH_CXXFLAGS) -c $< -o $@

%.o: %.cc $(HEADERS)
	$(CXX) $(CXXFLAGS) -c $< -o $@

test: test.cc $(OBJFILES)
	$(CXX) $(CXXFLAGS) -o test $^ $(LDFLAGS)

.PHONY: dbtest
dbtest: benchmarks/dbtest

benchmarks/dbtest: benchmarks/dbtest.cc $(OBJFILES) $(BENCH_OBJFILES)
	$(CXX) $(BENCH_CXXFLAGS) -o benchmarks/dbtest $^ $(BENCH_LDFLAGS)

.PHONY: kvtest
kvtest: benchmarks/masstree/kvtest

benchmarks/masstree/kvtest: benchmarks/masstree/kvtest.cc $(OBJFILES) $(BENCH_OBJFILES)
	$(CXX) $(BENCH_CXXFLAGS) -o benchmarks/masstree/kvtest $^ $(BENCH_LDFLAGS)

.PHONY: clean
clean:
	rm -f *.o test benchmarks/*.o benchmarks/dbtest benchmarks/masstree/*.o benchmarks/masstree/kvtest
