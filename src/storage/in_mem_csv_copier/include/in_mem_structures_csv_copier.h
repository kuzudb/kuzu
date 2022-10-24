#pragma once

#include "src/catalog/include/catalog.h"
#include "src/common/include/csv_reader/csv_reader.h"
#include "src/common/include/task_system/task_scheduler.h"
#include "src/storage/in_mem_storage_structure/include/in_mem_column.h"
#include "src/storage/in_mem_storage_structure/include/in_mem_lists.h"

namespace graphflow {
namespace storage {

using namespace graphflow::catalog;

class InMemStructuresCSVCopier {
protected:
    InMemStructuresCSVCopier(CSVDescription& csvDescription, string outputDirectory,
        TaskScheduler& taskScheduler, Catalog& catalog);

    virtual ~InMemStructuresCSVCopier() = default;

public:
    virtual void saveToFile() = 0;

protected:
    void calculateNumBlocks(const string& filePath, string tableName);

    uint64_t calculateNumRows(bool hasHeader);

    // Concurrent tasks
    static void countNumLinesPerBlockTask(
        const string& fName, uint64_t blockId, InMemStructuresCSVCopier* copier) {
        countNumLinesAndAdhocPropertiesPerBlockTask(fName, blockId, copier, UINT64_MAX, nullptr);
    }
    // We assume adhoc properties are written after predefined properties, so numTokensToSkip
    // indicates the start offset of adhoc properties.
    static void countNumLinesAndAdhocPropertiesPerBlockTask(const string& fName, uint64_t blockId,
        InMemStructuresCSVCopier* copier, uint64_t numTokensToSkip,
        unordered_map<string, DataType>* adhocProperties);

    // Initializes (in listHeadersBuilder) the header of each list in a Lists structure, from the
    // listSizes. ListSizes is used to determine if the list is small or large, based on which,
    // information is encoded in the 4 byte header.
    static void calculateListHeadersTask(node_offset_t numNodes, uint32_t elementSize,
        atomic_uint64_vec_t* listSizes, ListHeadersBuilder* listHeadersBuilder,
        const shared_ptr<spdlog::logger>& logger);
    // Initializes Metadata information of a Lists structure, that is chunksPagesMap and
    // largeListsPagesMap, using listSizes and listHeadersBuilder.
    // **Note that this file also allocates the in-memory pages of the InMemFile that will actually
    // stores the data in the lists (e.g., neighbor ids or edge properties or unstructured
    // properties).
    static void calculateListsMetadataAndAllocateInMemListPagesTask(uint64_t numNodes,
        uint32_t elementSize, atomic_uint64_vec_t* listSizes,
        ListHeadersBuilder* listHeadersBuilder, InMemLists* inMemList, bool hasNULLBytes,
        const shared_ptr<spdlog::logger>& logger);

protected:
    shared_ptr<spdlog::logger> logger;
    CSVDescription& csvDescription;
    string outputDirectory;
    uint64_t numBlocks;
    vector<uint64_t> numLinesPerBlock;
    TaskScheduler& taskScheduler;
    Catalog& catalog;
};

} // namespace storage
} // namespace graphflow
