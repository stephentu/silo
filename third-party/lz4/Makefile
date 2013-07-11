CC=gcc
CFLAGS=-I. -std=c99 -Wall -W -Wundef -Wno-implicit-function-declaration

OS := $(shell uname)
ifeq ($(OS),Linux)
EXT =
else
EXT =.exe
endif

default: lz4c

all: lz4c lz4cs lz4c32 fuzzer fullbench

lz4c: lz4.c lz4hc.c bench.c xxhash.c lz4c.c
	$(CC)      -O3 $(CFLAGS) $^ -o $@$(EXT)

lz4cs: lz4.c lz4hc.c bench.c xxhash.c lz4c.c
	$(CC)      -Os $(CFLAGS) $^ -o $@$(EXT)

lz4c32: lz4.c lz4hc.c bench.c xxhash.c lz4c.c
	$(CC) -m32 -O3 $(CFLAGS) $^ -o $@$(EXT)

fuzzer : lz4.c lz4hc.c fuzzer.c
	$(CC)      -O3 $(CFLAGS) $^ -o $@$(EXT)
	
fullbench : lz4.c lz4hc.c xxhash.c fullbench.c
	$(CC)      -O3 $(CFLAGS) $^ -o $@$(EXT)

.PHONY: library
library: liblz4.so

%.o: %.c
	$(CC) -fPIC -O3 $(CFLAGS) -c $< -o $@

liblz4.so: lz4.o xxhash.o
	$(CC) -shared -Wl,-soname,liblz4.so -o liblz4.so lz4.o xxhash.o

clean:
	rm -f core *.o *.so lz4c$(EXT) lz4cs$(EXT) lz4c32$(EXT) fuzzer$(EXT) fullbench$(EXT)
