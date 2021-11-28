#pragma once

#include <thread>
#include <unordered_set>
#include <vector>

#include "nlohmann/json.hpp"

#include "src/common/include/types.h"
#include "src/loader/include/dataset_metadata.h"
#include "src/loader/include/label_description.h"
#include "src/loader/include/thread_pool.h"
#include "src/storage/include/catalog.h"
#include "src/storage/include/graph.h"

using namespace graphflow::storage;
using namespace std;

namespace graphflow {
namespace loader {

const string UNSTR_PROPERTY_SEPARATOR = ":";
const string DEFAULT_METADATA_JSON_FILENAME = "metadata.json";

class GraphLoader {

public:
    GraphLoader(string inputDirectory, string outputDirectory, uint32_t numThreads);
    ~GraphLoader();

    void loadGraph();

private:
    void readAndParseMetadata(DatasetMetadata& metadata);
    void readCSVHeaderAndCalcNumBlocks(const vector<string>& filePaths,
        vector<uint64_t>& numBlocksPerFile, vector<string>& fileHeaders);
    void addNodeLabelsIntoGraphCatalog(
        const vector<NodeFileDescription>& fileDescriptions, vector<string>& fileHeaders);
    void addRelLabelsIntoGraphCatalog(
        const vector<RelFileDescription>& fileDescriptions, vector<string>& fileHeaders);
    vector<PropertyDefinition> parseHeader(string& header, char tokenSeparator) const;

    unique_ptr<vector<unique_ptr<NodeIDMap>>> loadNodes();

    void loadRels(vector<unique_ptr<NodeIDMap>>& nodeIDMaps);

    void countLinesAndAddUnstrPropertiesInCatalog(vector<vector<uint64_t>>& numLinesPerBlock,
        vector<vector<unordered_set<string>>>& labelBlockUnstrProperties,
        vector<uint64_t>& numBlocksPerLabel, const vector<string>& filePaths);

    void cleanup();

    // Concurrent Tasks

    static void countLinesAndScanUnstrPropertiesInBlockTask(const string& fName,
        const CSVSpecialChars& csvSpecialChars, uint32_t numStructuredProperties,
        unordered_set<string>* unstrPropertyNameSet, vector<vector<uint64_t>>* numLinesPerBlock,
        label_t label, uint32_t blockId, const shared_ptr<spdlog::logger>& logger);

private:
    shared_ptr<spdlog::logger> logger;
    ThreadPool threadPool;
    const string inputDirectory;
    const string outputDirectory;
    DatasetMetadata datasetMetadata;
    Graph graph;
};

} // namespace loader
} // namespace graphflow
