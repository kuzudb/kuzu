#include "src/storage/include/graph.h"

#include <fstream>

#include "bitsery/adapter/stream.h"
#include "bitsery/bitsery.h"
#include "bitsery/brief_syntax.h"
#include "bitsery/brief_syntax/vector.h"

using OutputStreamAdapter = bitsery::Serializer<bitsery::OutputBufferedStreamAdapter>;

namespace graphflow {
namespace storage {

Graph::Graph(const string &directory, uint64_t bufferPoolSize)
    : catalog(make_unique<Catalog>(directory)),
      bufferManager(make_unique<BufferManager>(bufferPoolSize)) {
    readFromFile(directory);
    nodePropertyStore =
        make_unique<NodePropertyStore>(*catalog, numNodesPerLabel, directory, *bufferManager);
    adjListIndexes =
        make_unique<AdjListsIndexes>(*catalog, numNodesPerLabel, directory, *bufferManager);
}

template<typename S>
void Graph::serialize(S &s) {
    s(numNodesPerLabel);
    s(numRelsPerLabel);
}

void Graph::saveToFile(const string &directory) {
    auto path = directory + "/graph.bin";
    fstream f{path, f.binary | f.trunc | f.out};
    if (!f.is_open()) {
        invalid_argument("Cannot open " + path + " for writing.");
    }
    OutputStreamAdapter serializer{f};
    serializer.object(*this);
    serializer.adapter().flush();
    f.close();
}

void Graph::readFromFile(const string &directory) {
    auto path = directory + "/graph.bin";
    fstream f{path, f.binary | f.in};
    if (!f.is_open()) {
        invalid_argument("Cannot open " + path + " for reading the graph.");
    }
    auto state = bitsery::quickDeserialization<bitsery::InputStreamAdapter>(f, *this);
    f.close();
    if (state.first == bitsery::ReaderError::NoError && state.second) {
        invalid_argument("Cannot deserialize the graph.");
    }
}

} // namespace storage
} // namespace graphflow
