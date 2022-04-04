#pragma once

#include <thread>
#include <unordered_set>
#include <vector>

#include "nlohmann/json.hpp"

#include "src/catalog/include/catalog.h"
#include "src/common/include/task_system/task_scheduler.h"
#include "src/loader/include/dataset_metadata.h"
#include "src/loader/include/label_description.h"
#include "src/loader/include/loader_progress_bar.h"
#include "src/storage/include/storage_manager.h"

using namespace graphflow::storage;
using namespace std;

namespace graphflow {
namespace loader {

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

    unique_ptr<vector<unique_ptr<NodeIDMap>>> loadNodes();

    void loadRels(unique_ptr<vector<unique_ptr<NodeIDMap>>> nodeIDMaps);

    void countLinesAndAddUnstrPropertiesInCatalog(vector<vector<uint64_t>>& numLinesPerBlock,
        vector<vector<unordered_set<string>>>& labelBlockUnstrProperties,
        vector<uint64_t>& numBlocksPerLabel, const vector<string>& filePaths);

    void cleanup();

    static vector<PropertyDefinition> parseCSVFileHeader(
        string& header, const LabelFileDescription& fileDescription);

    // Concurrent Tasks
    static void countLinesAndScanUnstrPropertiesInBlockTask(const string& fName,
        const CSVReaderConfig& csvReaderConfig, uint32_t numStructuredProperties,
        unordered_set<string>* unstrPropertyNameSet, vector<vector<uint64_t>>* numLinesPerBlock,
        label_t label, uint32_t blockId, const shared_ptr<spdlog::logger>& logger,
        LoaderProgressBar* progressBar);

private:
    shared_ptr<spdlog::logger> logger;
    unique_ptr<TaskScheduler> taskScheduler;
    const string inputDirectory;
    const string outputDirectory;
    unique_ptr<Catalog> catalog;
    DatasetMetadata datasetMetadata;
    unique_ptr<LoaderProgressBar> progressBar;
};

} // namespace loader
} // namespace graphflow
