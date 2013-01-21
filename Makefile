CXXFLAGS := -Wall -g -O2
LDFLAGS  := -lpthread -ljemalloc

HEADERS = btree.h macros.h rcu.h static_assert.h thread.h txn.h txn_btree.h varkey.h util.h \
	  spinbarrier.h
SRCFILES = btree.cc rcu.cc thread.cc txn.cc txn_btree.cc
OBJFILES = $(SRCFILES:.cc=.o)


BENCH_HEADERS = benchmarks/abstract_db.h benchmarks/bdb_wrapper.h benchmarks/ndb_wrapper.h
BENCH_SRCFILES = benchmarks/bdb_wrapper.cc benchmarks/ndb_wrapper.cc
BENCH_OBJFILES = $(BENCH_SRCFILES:.cc=.o)

all: test

%.o: %.cc $(HEADERS)
	$(CXX) $(CXXFLAGS) -c $< -o $@

benchmarks/%.o: benchmarks/%.cc $(HEADERS) $(BENCH_HEADERS)
	$(CXX) $(CXXFLAGS) -c $< -o $@

test: test.cc $(OBJFILES) 
	$(CXX) $(CXXFLAGS) -o test $^ $(LDFLAGS)

.PHONY: bench
bench: benchmarks/ycsb

benchmarks/ycsb: benchmarks/ycsb.cc $(OBJFILES) $(BENCH_OBJFILES)
	$(CXX) $(CXXFLAGS) -o benchmarks/ycsb $^ $(LDFLAGS) -ldb_cxx

.PHONY: clean
clean:
	rm -f *.o test benchmarks/*.o benchmarks/ycsb
