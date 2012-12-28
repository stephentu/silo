CXXFLAGS := -Wall -g

all: btree

btree: btree.cc static_assert.h util.h
	$(CXX) $(CXXFLAGS) -o btree btree.cc

.PHONY: clean
clean:
	rm -f btree
