#include "src/storage/include/graph.h"

#include <fstream>

#include "bitsery/adapter/stream.h"
#include "bitsery/bitsery.h"
#include "bitsery/brief_syntax.h"
#include "bitsery/brief_syntax/vector.h"
#include "spdlog/sinks/stdout_sinks.h"

using OutputStreamAdapter = bitsery::Serializer<bitsery::OutputBufferedStreamAdapter>;

namespace graphflow {
namespace storage {

Graph::Graph(const string& path, uint64_t bufferPoolSize)
    : logger{spdlog::stdout_logger_mt("storage")}, path{path} {
    logger->info("Initializing Graph.");
    catalog = make_unique<Catalog>(path);
    bufferManager = make_unique<BufferManager>(bufferPoolSize);
    readFromFile(path);
    nodesStore = make_unique<NodesStore>(*catalog, numNodesPerLabel, path, *bufferManager);
    relsStore = make_unique<RelsStore>(*catalog, numNodesPerLabel, path, *bufferManager);
    logger->info("Done.");
    return;
}

template<typename S>
void Graph::serialize(S& s) {
    s(numNodesPerLabel);
}

void Graph::saveToFile(const string& path) {
    auto graphPath = path + "/graph.bin";
    fstream f{graphPath, f.binary | f.trunc | f.out};
    if (f.fail()) {
        throw invalid_argument("Cannot open " + graphPath + " for writing.");
    }
    OutputStreamAdapter serializer{f};
    serializer.object(*this);
    serializer.adapter().flush();
    f.close();
}

void Graph::readFromFile(const string& path) {
    auto graphPath = path + "/graph.bin";
    logger->debug("Reading from {}.", graphPath);
    fstream f{graphPath, f.binary | f.in};
    if (f.fail()) {
        throw invalid_argument("Cannot open " + graphPath + " for reading the graph.");
    }
    auto state = bitsery::quickDeserialization<bitsery::InputStreamAdapter>(f, *this);
    f.close();
    if (!(state.first == bitsery::ReaderError::NoError && state.second)) {
        throw invalid_argument("Cannot deserialize the graph.");
    }
}

unique_ptr<nlohmann::json> Graph::debugInfo() {
    logger->info("PrintGraphInfo.");
    logger->info("path to db files: " + getPath());
    auto json = catalog->debugInfo();
    for (uint64_t labelIdx = 0; labelIdx < numNodesPerLabel.size(); ++labelIdx) {
        (*json)["NodeLabelSizes"][catalog->getStringNodeLabel(labelIdx)]["numNodes"] =
            to_string(numNodesPerLabel.at(labelIdx));
    }
    (*json).merge_patch(*(bufferManager->debugInfo()));
    return json;
}

} // namespace storage
} // namespace graphflow
