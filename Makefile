all: btree

btree: btree.cc static_assert.h util.h
	$(CXX) -Wall -g -o btree btree.cc

.PHONY: clean
clean:
	rm -f btree
