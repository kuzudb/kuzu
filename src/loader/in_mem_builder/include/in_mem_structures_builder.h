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
    InMemStructuresBuilder(string labelName, string inputFilePath, string outputDirectory,
        CSVReaderConfig csvReaderConfig, TaskScheduler& taskScheduler,
        CatalogBuilder& catalogBuilder, LoaderProgressBar* progressBar);

    virtual ~InMemStructuresBuilder() = default;

public:
    virtual void saveToFile() = 0;

protected:
    vector<PropertyNameDataType> parseCSVHeader(const string& filePath);

    void calculateNumBlocks(const string& filePath);

    uint64_t calculateNumRowsWithoutHeader();

    // Concurrent tasks
    static void countNumLinesPerBlockTask(
        const string& fName, uint64_t blockId, InMemStructuresBuilder* builder) {
        countNumLinesAndUnstrPropertiesPerBlockTask(fName, blockId, builder, UINT64_MAX, nullptr);
    }
    // We assume unstructured properties are written after structured properties, so numTokensToSkip
    // indicates the start offset of unstructured properties.
    static void countNumLinesAndUnstrPropertiesPerBlockTask(const string& fName, uint64_t blockId,
        InMemStructuresBuilder* builder, uint64_t numTokensToSkip,
        unordered_set<string>* unstrPropertyNames);
    // Initializes (in listHeadersBuilder) the header of each list in a Lists structure, from the
    // listSizes. ListSizes is used to determine if the list is small or large, based on which,
    // information is encoded in the 4 byte header.
    static void calculateListHeadersTask(node_offset_t numNodes, uint32_t elementSize,
        atomic_uint64_vec_t* listSizes, ListHeadersBuilder* listHeadersBuilder,
        const shared_ptr<spdlog::logger>& logger, LoaderProgressBar* progressBar);
    // Initializes Metadata information of a Lists structure, that is chunksPagesMap and
    // largeListsPagesMap, using listSizes and listHeadersBuilder.
    // **Note that this file also allocates the in-memory pages of the InMemFile that will actually
    // stores the data in the lists (e.g., neighbor ids or edge properties or unstructured
    // properties).
    static void calculateListsMetadataAndAllocateInMemListPagesTask(uint64_t numNodes,
        uint32_t elementSize, atomic_uint64_vec_t* listSizes,
        ListHeadersBuilder* listHeadersBuilder, InMemLists* inMemList, bool hasNULLBytes,
        const shared_ptr<spdlog::logger>& logger, LoaderProgressBar* progressBar);

private:
    vector<PropertyNameDataType> parseHeaderLine(string& header) const;

protected:
    shared_ptr<spdlog::logger> logger;
    string labelName;
    string inputFilePath;
    string outputDirectory;
    uint64_t numBlocks;
    vector<uint64_t> numLinesPerBlock;
    CSVReaderConfig csvReaderConfig;
    TaskScheduler& taskScheduler;
    CatalogBuilder& catalogBuilder;
    LoaderProgressBar* progressBar;
};

} // namespace loader
} // namespace graphflow
