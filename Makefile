CXXFLAGS := -Wall -g -O2
LDFLAGS  := -lpthread -ljemalloc

HEADERS = btree.h macros.h rcu.h static_assert.h thread.h util.h
SRCFILES = btree.cc rcu.cc thread.cc
OBJFILES = $(SRCFILES:.cc=.o)

all: btree

%.o: %.cc 
	$(CXX) $(CXXFLAGS) -c $^ -o $@

btree: $(OBJFILES) 
	$(CXX) $(CXXFLAGS) -o btree $^ $(LDFLAGS)

.PHONY: clean
clean:
	rm -f *.o btree
