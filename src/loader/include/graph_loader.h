#pragma once

#include <thread>
#include <vector>

#include "src/catalog/include/catalog.h"
#include "src/common/include/task_system/task_scheduler.h"
#include "src/loader/include/dataset_metadata.h"
#include "src/loader/include/loader_progress_bar.h"
#include "src/loader/include/node_id_map.h"

using namespace graphflow::storage;
using namespace graphflow::catalog;

namespace graphflow {
namespace loader {

class GraphLoader {

public:
    GraphLoader(string inputDirectory, string outputDirectory, uint32_t numThreads);
    ~GraphLoader();

    void loadGraph();

private:
    void readAndParseMetadata(DatasetMetadata& metadata);

    vector<unique_ptr<NodeIDMap>> loadNodes();
    void loadRels(const vector<unique_ptr<NodeIDMap>>& nodeIDMaps);

    void cleanup();

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
