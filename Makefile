CXXFLAGS := -Wall -g -O2
LDFLAGS  := -lpthread -ljemalloc

HEADERS = btree.h macros.h rcu.h static_assert.h thread.h txn.h txn_btree.h varkey.h util.h
SRCFILES = btree.cc rcu.cc thread.cc txn.cc txn_btree.cc
OBJFILES = $(SRCFILES:.cc=.o)

all: test

%.o: %.cc $(HEADERS)
	$(CXX) $(CXXFLAGS) -c $< -o $@

test: test.cc $(OBJFILES) 
	$(CXX) $(CXXFLAGS) -o test $^ $(LDFLAGS)

benchmarks/ycsb: benchmarks/ycsb.cc $(OBJFILES)
	$(CXX) $(CXXFLAGS) -o benchmarks/ycsb $^ $(LDFLAGS) -ldb_cxx

.PHONY: clean
clean:
	rm -f *.o test benchmarks/ycsb
