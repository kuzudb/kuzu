CXX=g++
RM=rm -rf

# CPPFLAGS=-O0 -g -Ithrift -I. -std=c++11 -fPIC -Wall -fsanitize=address
# LDFLAGS=-O0 -g -fsanitize=address

CPPFLAGS=-O3 -g -Isrc/thrift -Isrc -std=c++11 -fPIC -Wall
LDFLAGS=-O3 -g


SOEXT=so
UNAME_S := $(shell uname -s)

ifeq ($(UNAME_S),Darwin)
	SOEXT = dylib
endif


OBJS=src/parquet/parquet_constants.o src/parquet/parquet_types.o src/thrift/protocol/TProtocol.o  src/thrift/transport/TTransportException.o src/thrift/transport/TBufferTransports.o src/snappy/snappy.o src/snappy/snappy-sinksource.o src/miniparquet.o

all: libminiparquet.$(SOEXT) pq2csv pqbench

libminiparquet.$(SOEXT): $(OBJS)
	$(CXX) $(LDFLAGS) -shared -o libminiparquet.$(SOEXT) $(OBJS) 


pq2csv: libminiparquet.$(SOEXT) pq2csv.o
	$(CXX) $(LDFLAGS) -o pq2csv $(OBJS) pq2csv.o 

pqbench: libminiparquet.$(SOEXT) bench.o
	$(CXX) $(LDFLAGS) -o pqbench $(OBJS) bench.o 

clean:
	$(RM) $(OBJS) pq2csv pq2csv.o pqbench pqbench.o libminiparquet.$(SOEXT) *.dSYM

test: pq2csv
	./test.sh
