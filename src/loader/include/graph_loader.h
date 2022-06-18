#pragma once

#include <thread>
#include <vector>

#include "src/catalog/include/catalog.h"
#include "src/common/include/task_system/task_scheduler.h"
#include "src/loader/include/dataset_metadata.h"
#include "src/loader/include/loader_progress_bar.h"
#include "src/storage/index/include/hash_index.h"

using namespace graphflow::storage;
using namespace graphflow::catalog;

namespace graphflow {
namespace loader {

class GraphLoader {

public:
    GraphLoader(string inputDirectory, string outputDirectory, uint32_t numThreads,
        uint64_t defaultPageBufferPoolSize, uint64_t largePageBufferPoolSize);
    ~GraphLoader();

    void loadGraph();

private:
    void readAndParseMetadata(DatasetMetadata& metadata);

    vector<unique_ptr<HashIndex>> loadNodes();
    void loadRels(const vector<node_offset_t>& maxNodeOffsetsPerNodeLabel,
        const vector<unique_ptr<HashIndex>>& IDIndexes);

    void cleanup();

private:
    shared_ptr<spdlog::logger> logger;
    unique_ptr<TaskScheduler> taskScheduler;
    const string inputDirectory;
    const string outputDirectory;
    unique_ptr<Catalog> catalog;
    unique_ptr<BufferManager> bufferManager;
    DatasetMetadata datasetMetadata;
    vector<node_offset_t> maxNodeOffsetsPerNodeLabel;
    unique_ptr<LoaderProgressBar> progressBar;
};

} // namespace loader
} // namespace graphflow
