-include config.mk

### Options ###

DEBUG ?= 0
CHECK_INVARIANTS ?= 0

# 0 = libc malloc
# 1 = jemalloc
# 2 = tcmalloc
# 3 = flow
USE_MALLOC_MODE ?= 1

MYSQL ?= 1
MYSQL_SHARE_DIR ?= /x/stephentu/mysql-5.5.29/build/sql/share

# Available modes
#   * perf
#   * backoff
#   * factor-gc
#   * factor-gc-nowriteinplace
#   * sandbox
MODE ?= perf

###############

DEBUG_S=$(strip $(DEBUG))
CHECK_INVARIANTS_S=$(strip $(CHECK_INVARIANTS))
EVENT_COUNTERS_S=$(strip $(EVENT_COUNTERS))
USE_MALLOC_MODE_S=$(strip $(USE_MALLOC_MODE))
MODE_S=$(strip $(MODE))
MYSQL_S=$(strip $(MYSQL))

ifeq ($(DEBUG_S),1)
	OSUFFIX_D=.debug
endif
ifeq ($(CHECK_INVARIANTS_S),1)
	OSUFFIX_S=.check
endif
ifeq ($(EVENT_COUNTERS_S),1)
	OSUFFIX_E=.ectrs
endif
OSUFFIX=$(OSUFFIX_D)$(OSUFFIX_S)$(OSUFFIX_E)

ifeq ($(MODE_S),perf)
	O := out-perf$(OSUFFIX)
	CONFIG_H = config/config-perf.h
else ifeq ($(MODE_S),backoff)
	O := out-backoff$(OSUFFIX)
	CONFIG_H = config/config-backoff.h
else ifeq ($(MODE_S),factor-gc)
	O := out-factor-gc$(OSUFFIX)
	CONFIG_H = config/config-factor-gc.h
else ifeq ($(MODE_S),factor-gc-nowriteinplace)
	O := out-factor-gc-nowriteinplace$(OSUFFIX)
	CONFIG_H = config/config-factor-gc-nowriteinplace.h
else ifeq ($(MODE_S),factor-fake-compression)
	O := out-factor-fake-compression$(OSUFFIX)
	CONFIG_H = config/config-factor-fake-compression.h
else ifeq ($(MODE_S),sandbox)
	O := out-sandbox$(OSUFFIX)
	CONFIG_H = config/config-sandbox.h
else
	$(error invalid mode)
endif

CXXFLAGS := -g -Wall -Werror -std=c++0x
CXXFLAGS += -MD -Ithird-party/lz4 -DCONFIG_H=\"$(CONFIG_H)\"
ifeq ($(DEBUG_S),1)
        CXXFLAGS += -fno-omit-frame-pointer -DDEBUG
else
        CXXFLAGS += -O2 -funroll-loops -fno-omit-frame-pointer
endif
ifeq ($(CHECK_INVARIANTS_S),1)
	CXXFLAGS += -DCHECK_INVARIANTS
endif
ifeq ($(EVENT_COUNTERS_S),1)
	CXXFLAGS += -DENABLE_EVENT_COUNTERS
endif

TOP     := $(shell echo $${PWD-`pwd`})
LDFLAGS := -lpthread -lnuma -lrt

LZ4LDFLAGS := -Lthird-party/lz4 -llz4 -Wl,-rpath,$(TOP)/third-party/lz4

ifeq ($(USE_MALLOC_MODE_S),1)
        CXXFLAGS+=-DUSE_JEMALLOC
        LDFLAGS+=-ljemalloc
else ifeq ($(USE_MALLOC_MODE_S),2)
        CXXFLAGS+=-DUSE_TCMALLOC
        LDFLAGS+=-ltcmalloc
else ifeq ($(USE_MALLOC_MODE_S),3)
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

BENCH_CXXFLAGS := $(CXXFLAGS)
BENCH_LDFLAGS := $(LDFLAGS) -ldb_cxx -lz -lrt -lcrypt -laio -ldl -lssl -lcrypto

BENCH_SRCFILES = benchmarks/bdb_wrapper.cc \
	benchmarks/bench.cc \
	benchmarks/encstress.cc \
	benchmarks/masstree/kvrandom.cc \
	benchmarks/queue.cc \
	benchmarks/tpcc.cc \
	benchmarks/ycsb.cc

ifeq ($(MYSQL_S),1)
BENCH_CXXFLAGS += -DMYSQL_SHARE_DIR=\"$(MYSQL_SHARE_DIR)\"
BENCH_LDFLAGS := -L/usr/lib/mysql -lmysqld $(BENCH_LDFLAGS)
BENCH_SRCFILES += benchmarks/mysql_wrapper.cc
else
BENCH_CXXFLAGS += -DNO_MYSQL
endif

BENCH_OBJFILES := $(patsubst %.cc, $(O)/%.o, $(BENCH_SRCFILES))

NEWBENCH_SRCFILES = new-benchmarks/bench.cc \
	new-benchmarks/tpcc.cc

NEWBENCH_OBJFILES := $(patsubst %.cc, $(O)/%.o, $(NEWBENCH_SRCFILES))

all: $(O)/test

$(O)/benchmarks/%.o: benchmarks/%.cc $(O)/buildstamp $(O)/buildstamp.bench $(OBJDEP)
	@mkdir -p $(@D)
	$(CXX) $(BENCH_CXXFLAGS) -c $< -o $@

$(O)/benchmarks/masstree/%.o: benchmarks/masstree/%.cc $(O)/buildstamp $(O)/buildstamp.bench $(OBJDEP)
	@mkdir -p $(@D)
	$(CXX) $(BENCH_CXXFLAGS) -c $< -o $@

$(O)/new-benchmarks/%.o: new-benchmarks/%.cc $(O)/buildstamp $(O)/buildstamp.bench $(OBJDEP)
	@mkdir -p $(@D)
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(O)/%.o: %.cc $(O)/buildstamp $(OBJDEP)
	@mkdir -p $(@D)
	$(CXX) $(CXXFLAGS) -c $< -o $@

third-party/lz4/liblz4.so:
	make -C third-party/lz4 library

.PHONY: test
test: $(O)/test

$(O)/test: $(O)/test.o $(OBJFILES) third-party/lz4/liblz4.so
	$(CXX) -o $(O)/test $^ $(LDFLAGS) $(LZ4LDFLAGS)

.PHONY: persist_test
persist_test: $(O)/persist_test

$(O)/persist_test: $(O)/persist_test.o third-party/lz4/liblz4.so
	$(CXX) -o $(O)/persist_test $(O)/persist_test.o $(LDFLAGS) $(LZ4LDFLAGS)

.PHONY: stats_client
stats_client: $(O)/stats_client

$(O)/stats_client: $(O)/stats_client.o
	$(CXX) -o $(O)/stats_client $(O)/stats_client.o $(LDFLAGS)

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

DEPFILES := $(wildcard $(O)/*.d $(O)/*/*.d $(O)/*/*/*.d)
ifneq ($(DEPFILES),)
-include $(DEPFILES)
endif

ifneq ($(strip $(DEBUG_S).$(CHECK_INVARIANTS_S).$(EVENT_COUNTERS_S)),$(strip $(DEP_MAIN_CONFIG)))
DEP_MAIN_CONFIG := $(shell mkdir -p $(O); echo >$(O)/buildstamp; echo "DEP_MAIN_CONFIG:=$(DEBUG_S).$(CHECK_INVARIANTS_S).$(EVENT_COUNTERS_S)" >$(O)/_main_config.d)
endif

ifneq ($(strip $(MYSQL_S)),$(strip $(DEP_BENCH_CONFIG)))
DEP_BENCH_CONFIG := $(shell mkdir -p $(O); echo >$(O)/buildstamp.bench; echo "DEP_BENCH_CONFIG:=$(MYSQL_S)" >$(O)/_bench_config.d)
endif

$(O)/buildstamp $(O)/buildstamp.bench:
	@mkdir -p $(@D)
	@echo >$@

.PHONY: clean
clean:
	rm -rf out-*
	make -C third-party/lz4 clean
