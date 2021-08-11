#include "src/storage/include/graph.h"

#include <fstream>

#include "spdlog/sinks/stdout_sinks.h"
#include "spdlog/spdlog.h"

#include "src/storage/include/buffer_manager.h"

namespace graphflow {
namespace common {

/**
 * Specialized serialize and deserialize functions used in Catalog.
 * */

template<>
uint64_t SerDeser::serializeVector<vector<uint64_t>>(
    const vector<vector<uint64_t>>& values, FileInfo* fileInfo, uint64_t offset) {
    uint64_t vectorSize = values.size();
    offset = SerDeser::serializeValue<uint64_t>(vectorSize, fileInfo, offset);
    for (auto& value : values) {
        offset = serializeVector<uint64_t>(value, fileInfo, offset);
    }
    return offset;
}

template<>
uint64_t SerDeser::deserializeVector<vector<uint64_t>>(
    vector<vector<uint64_t>>& values, FileInfo* fileInfo, uint64_t offset) {
    uint64_t vectorSize;
    offset = deserializeValue<uint64_t>(vectorSize, fileInfo, offset);
    values.resize(vectorSize);
    for (auto& value : values) {
        offset = SerDeser::deserializeVector<uint64_t>(value, fileInfo, offset);
    }
    return offset;
}

template<>
uint64_t SerDeser::serializeVector<vector<vector<uint64_t>>>(
    const vector<vector<vector<uint64_t>>>& values, FileInfo* fileInfo, uint64_t offset) {
    uint64_t vectorSize = values.size();
    offset = SerDeser::serializeValue<uint64_t>(vectorSize, fileInfo, offset);
    for (auto& value : values) {
        offset = serializeVector<vector<uint64_t>>(value, fileInfo, offset);
    }
    return offset;
}

template<>
uint64_t SerDeser::deserializeVector<vector<vector<uint64_t>>>(
    vector<vector<vector<uint64_t>>>& values, FileInfo* fileInfo, uint64_t offset) {
    uint64_t vectorSize;
    offset = deserializeValue<uint64_t>(vectorSize, fileInfo, offset);
    values.resize(vectorSize);
    for (auto& value : values) {
        offset = SerDeser::deserializeVector<vector<uint64_t>>(value, fileInfo, offset);
    }
    return offset;
}

} // namespace common
} // namespace graphflow

namespace graphflow {
namespace storage {

Graph::Graph(const string& path, BufferManager& bufferManager, bool isInMemoryMode)
    : logger{LoggerUtils::getOrCreateSpdLogger("storage")}, path{path} {
    logger->info("Initializing Graph.");
    catalog = make_unique<Catalog>(path);
    readFromFile(path);
    nodesStore =
        make_unique<NodesStore>(*catalog, numNodesPerLabel, path, bufferManager, isInMemoryMode);
    relsStore =
        make_unique<RelsStore>(*catalog, numNodesPerLabel, path, bufferManager, isInMemoryMode);
    logger->info("Done.");
}

Graph::Graph() {
    logger = LoggerUtils::getOrCreateSpdLogger("storage");
}

Graph::~Graph() {
    spdlog::drop("storage");
}

void Graph::saveToFile(const string& path) const {
    auto graphPath = path + "/graph.bin";
    auto fileInfo = FileUtils::openFile(graphPath, O_WRONLY | O_CREAT);
    uint64_t offset = 0;
    offset = SerDeser::serializeVector(numNodesPerLabel, fileInfo.get(), offset);
    SerDeser::serializeVector<vector<vector<uint64_t>>>(
        numRelsPerDirBoundLabelRelLabel, fileInfo.get(), offset);
    FileUtils::closeFile(fileInfo->fd);
}

void Graph::readFromFile(const string& path) {
    auto graphPath = path + "/graph.bin";
    auto fileInfo = FileUtils::openFile(graphPath, O_RDONLY);
    uint64_t offset = 0;
    offset = SerDeser::deserializeVector(numNodesPerLabel, fileInfo.get(), offset);
    SerDeser::deserializeVector<vector<vector<uint64_t>>>(
        numRelsPerDirBoundLabelRelLabel, fileInfo.get(), offset);
    FileUtils::closeFile(fileInfo->fd);
}

unique_ptr<nlohmann::json> Graph::debugInfo() {
    logger->info("PrintGraphInfo.");
    logger->info("path to db files: " + getPath());
    auto json = catalog->debugInfo();
    (*json)["path"] = getPath();
    for (uint64_t labelIdx = 0; labelIdx < numNodesPerLabel.size(); ++labelIdx) {
        (*json)["NodeLabelSizes"][catalog->getNodeLabelName(labelIdx)]["numNodes"] =
            to_string(numNodesPerLabel.at(labelIdx));
    }
    return json;
}

} // namespace storage
} // namespace graphflow
