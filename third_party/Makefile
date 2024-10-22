CXX=g++
RM=rm -rf

# CPPFLAGS=-O0 -g -Ithrift -I. -std=c++11 -fPIC -Wall -fsanitize=address
# LDFLAGS=-O0 -g -fsanitize=address

CPPFLAGS=-O3 -g -Ithrift -Isrc -std=c++11 -fPIC -Wall
LDFLAGS=-O3 -g


SOEXT=so
UNAME_S := $(shell uname -s)

ifeq ($(UNAME_S),Darwin)
	SOEXT = dylib
endif


OBJS=parquet/parquet_constants.o parquet/parquet_types.o thrift/protocol/TProtocol.o  thrift/transport/TTransportException.o thrift/transport/TBufferTransports.o snappy/snappy.o snappy/snappy-sinksource.o parquet.o

all: libparquet.$(SOEXT) pq2csv pqbench

libparquet.$(SOEXT): $(OBJS)
	$(CXX) $(LDFLAGS) -shared -o libparquet.$(SOEXT) $(OBJS) 


pq2csv: libparquet.$(SOEXT) pq2csv.o
	$(CXX) $(LDFLAGS) -o pq2csv $(OBJS) pq2csv.o 

pqbench: libparquet.$(SOEXT) bench.o
	$(CXX) $(LDFLAGS) -o pqbench $(OBJS) bench.o 

clean:
	$(RM) $(OBJS) pq2csv pq2csv.o pqbench pqbench.o libparquet.$(SOEXT) *.dSYM

test: pq2csv
	./test.sh