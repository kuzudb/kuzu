#pragma once

#include "src/catalog/include/catalog.h"
#include "src/common/include/task_system/task_scheduler.h"
#include "src/loader/in_mem_storage_structure/include/in_mem_column.h"
#include "src/loader/in_mem_storage_structure/include/in_mem_lists.h"
#include "src/loader/include/dataset_metadata.h"
#include "src/loader/include/loader_progress_bar.h"

namespace graphflow {
namespace loader {

using namespace graphflow::catalog;

using label_adj_columns_map_t = unordered_map<label_t, unique_ptr<InMemAdjColumn>>;
using label_property_columns_map_t = unordered_map<label_t, vector<unique_ptr<InMemColumn>>>;
using label_property_lists_map_t = unordered_map<label_t, vector<unique_ptr<InMemLists>>>;
using label_adj_lists_map_t = unordered_map<label_t, unique_ptr<InMemAdjLists>>;

class InMemStructuresBuilder {

protected:
    InMemStructuresBuilder(label_t label, string labelName, string inputFilePath,
        string outputDirectory, CSVReaderConfig csvReaderConfig, TaskScheduler& taskScheduler,
        Catalog& catalog, LoaderProgressBar* progressBar);

    virtual ~InMemStructuresBuilder() = default;

public:
    virtual void saveToFile() = 0;

protected:
    uint64_t parseCSVHeaderAndCalcNumBlocks(
        const string& filePath, vector<PropertyNameDataType>& colDefinitions);

    // Concurrent tasks
    // Initializes (in listHeaders) the header of each list in a Lists structure, from the
    // listSizes. ListSizes is used to determine if the list is small or large, based on which,
    // information is encoded in the 4 byte header.
    static void calculateListHeadersTask(node_offset_t numNodes, uint32_t elementSize,
        atomic_uint64_vec_t* listSizes, ListHeaders* listHeaders,
        const shared_ptr<spdlog::logger>& logger, LoaderProgressBar* progressBar);
    // Initializes Metadata information of a Lists structure, that is chunksPagesMap and
    // largeListsPagesMap, using listSizes and listHeaders.
    static void calculateListsMetadataTask(uint64_t numNodes, uint32_t elementSize,
        atomic_uint64_vec_t* listSizes, ListHeaders* listHeaders, ListsMetadata* listsMetadata,
        bool hasNULLBytes, const shared_ptr<spdlog::logger>& logger,
        LoaderProgressBar* progressBar);

private:
    vector<PropertyNameDataType> parseCSVFileHeader(string& header) const;

protected:
    shared_ptr<spdlog::logger> logger;
    label_t label;
    string labelName;
    string inputFilePath;
    string outputDirectory;
    uint64_t numBlocks;
    CSVReaderConfig csvReaderConfig;
    TaskScheduler& taskScheduler;
    Catalog& catalog;
    LoaderProgressBar* progressBar;
};

} // namespace loader
} // namespace graphflow
