#pragma once

#include "common/copier_config/copier_config.h"
#include "common/logging_level_utils.h"
#include "common/task_system/task_scheduler.h"
#include "node_copier.h"
#include "npy_reader.h"
#include "storage/in_mem_storage_structure/in_mem_column.h"
#include "storage/index/hash_index_builder.h"
#include "storage/store/nodes_statistics_and_deleted_ids.h"

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace storage {

class NpyNodeCopier : public NodeCopier {

public:
    NpyNodeCopier(CopyDescription& copyDescription, std::string outputDirectory,
        TaskScheduler& taskScheduler, catalog::Catalog& catalog, table_id_t tableID,
        NodesStatisticsAndDeletedIDs* nodesStatisticsAndDeletedIDs)
        : NodeCopier(copyDescription, outputDirectory, taskScheduler, catalog, tableID,
              nodesStatisticsAndDeletedIDs){};

    ~NpyNodeCopier() override = default;

private:
    void populateColumnsAndLists() override;

    void populateInMemoryStructures() override;

    void initializeNpyReaders();

    void validateNpyReaders();

    void populateColumnsFromNpy(std::unique_ptr<HashIndexBuilder<int64_t>>& pkIndex);

    void assignCopyNpyTasks(
        common::property_id_t propertyIdx, std::unique_ptr<HashIndexBuilder<int64_t>>& pkIndex);

    static void batchPopulateColumnsTask(common::property_id_t primaryKeyPropertyIdx,
        uint64_t blockIdx, offset_t startOffset, uint64_t numLinesInCurBlock,
        HashIndexBuilder<int64_t>* pkIndex, NpyNodeCopier* copier,
        common::property_id_t propertyIdx);

private:
    std::unordered_map<common::property_id_t, std::unique_ptr<NpyReader>> npyReaderMap;
};

} // namespace storage
} // namespace kuzu
