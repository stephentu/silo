CXXFLAGS := -Wall -g
LDFLAGS  := -lpthread -ljemalloc

HEADERS = btree.h macros.h rcu.h static_assert.h thread.h txn.h txn_btree.h util.h
SRCFILES = btree.cc rcu.cc thread.cc txn.cc txn_btree.cc
OBJFILES = $(SRCFILES:.cc=.o)

all: btree

%.o: %.cc 
	$(CXX) $(CXXFLAGS) -c $^ -o $@

btree: $(OBJFILES) 
	$(CXX) $(CXXFLAGS) -o btree $^ $(LDFLAGS)

.PHONY: clean
clean:
	rm -f *.o btree
