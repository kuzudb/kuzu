#pragma once

#include <thread>
#include <unordered_set>
#include <vector>

#include "nlohmann/json.hpp"

#include "src/common/include/types.h"
#include "src/loader/include/thread_pool.h"
#include "src/loader/include/utils.h"
#include "src/storage/include/catalog.h"
#include "src/storage/include/graph.h"

using namespace graphflow::storage;
using namespace std;

namespace graphflow {
namespace loader {

class GraphLoader {

    typedef vector<vector<unordered_set<const char*, charArrayHasher, charArrayEqualTo>>>
        labelBlockUnstrPropertyKeys_t;

public:
    GraphLoader(const string& inputDirectory, const string& outputDirectory, uint32_t numThreads);
    ~GraphLoader();

    void loadGraph();

private:
    unique_ptr<nlohmann::json> readMetadata();

    void assignIdxToLabels(stringToLabelMap_t& map, const nlohmann::json& fileDescriptions);
    void setCardinalitiesOfRelLabels(const nlohmann::json& metadata);
    void setSrcDstNodeLabelsForRelLabels(const nlohmann::json& metadata);

    void checkNodePrimaryKeyConstraints();
    unique_ptr<vector<unique_ptr<NodeIDMap>>> loadNodes(const nlohmann::json& metadata);

    void loadRels(const nlohmann::json& metadata, vector<unique_ptr<NodeIDMap>>& nodeIDMaps);

    void inferFnamesFromMetadataFileDesriptions(
        label_t numLabels, nlohmann::json fileDescriptions, vector<string>& filenames);
    vector<string> getNodePrimaryKeysFromMetadata(
        label_t numLabels, nlohmann::json fileDescriptions);

    void initPropertyKeyMapAndCalcNumBlocks(label_t numLabels, vector<string>& filenames,
        vector<uint64_t>& numBlocksPerLabel,
        vector<unordered_map<string, PropertyKey>>& propertyKeysMaps, const char tokenSeparator);

    void initNodePropertyPrimaryKeys(vector<string>& primaryKeys);

    void parseHeader(const char tokenSeparator, string& header,
        unordered_map<string, PropertyKey>& propertyKeyMap);

    void countLinesAndGetUnstrPropertyKeys(vector<vector<uint64_t>>& numLinesPerBlock,
        labelBlockUnstrPropertyKeys_t& labelBlockUnstrPropertyKeys,
        vector<uint64_t>& numBlocksPerLabel, const char tokenSeparator, vector<string>& fnames);

    void initNodeUnstrPropertyKeyMaps(labelBlockUnstrPropertyKeys_t& labelBlockUnstrPropertyKeys);

    // Concurrent Tasks

    static void countLinesAndScanUnstrPropertiesInBlockTask(string fname, char tokenSeparator,
        const uint32_t numProperties,
        unordered_set<const char*, charArrayHasher, charArrayEqualTo>* unstrPropertyKeys,
        vector<vector<uint64_t>>* numLinesPerBlock, label_t label, uint32_t blockId,
        shared_ptr<spdlog::logger> logger);

private:
    shared_ptr<spdlog::logger> logger;
    ThreadPool threadPool;
    const string inputDirectory;
    const string outputDirectory;

    Graph graph;
};

} // namespace loader
} // namespace graphflow
