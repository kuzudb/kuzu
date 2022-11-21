#pragma once

#include "catalog/catalog.h"
#include "common/csv_reader/csv_reader.h"
#include "common/logging_level_utils.h"
#include "common/task_system/task_scheduler.h"
#include "storage/in_mem_storage_structure/in_mem_column.h"
#include "storage/in_mem_storage_structure/in_mem_lists.h"

namespace kuzu {
namespace storage {

using namespace kuzu::catalog;

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
        const string& fName, uint64_t blockId, InMemStructuresCSVCopier* copier);
    // Initializes (in listHeadersBuilder) the header of each list in a Lists structure, from the
    // listSizes. ListSizes is used to determine if the list is small or large, based on which,
    // information is encoded in the 4 byte header.
    static void calculateListHeadersTask(node_offset_t numNodes, uint32_t elementSize,
        atomic_uint64_vec_t* listSizes, ListHeadersBuilder* listHeadersBuilder,
        const shared_ptr<spdlog::logger>& logger);
    // Initializes Metadata information of a Lists structure, that is chunksPagesMap and
    // largeListsPagesMap, using listSizes and listHeadersBuilder.
    // **Note that this file also allocates the in-memory pages of the InMemFile that will actually
    // stores the data in the lists (e.g., neighbor ids or edge properties).
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
} // namespace kuzu
