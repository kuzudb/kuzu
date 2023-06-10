#pragma once

#include "storage/copier/rel_copier.h"
#include "storage/copier/table_copy_utils.h"

namespace kuzu {
namespace storage {

struct DirectedInMemRelColumns {
    std::unique_ptr<InMemColumnChunk> adjColumnChunk;
    std::unique_ptr<InMemColumn> adjColumn;
    std::unordered_map<common::property_id_t, std::unique_ptr<InMemColumnChunk>>
        propertyColumnChunks;
    std::unordered_map<common::property_id_t, std::unique_ptr<InMemColumn>> propertyColumns;
};

struct DirectedInMemRelLists {
    std::unique_ptr<atomic_uint64_vec_t> relListsSizes;
    std::unique_ptr<InMemAdjLists> adjList;
    std::unordered_map<common::property_id_t, std::unique_ptr<InMemLists>> propertyLists;
};

class DirectedInMemRelData {
public:
    void setColumns(std::unique_ptr<DirectedInMemRelColumns> columns_) {
        isColumns = true;
        this->columns = std::move(columns_);
    }

    void setRelLists(std::unique_ptr<DirectedInMemRelLists> lists_) {
        isColumns = false;
        this->lists = std::move(lists_);
    }

public:
    bool isColumns;
    std::unique_ptr<DirectedInMemRelColumns> columns;
    std::unique_ptr<DirectedInMemRelLists> lists;
};

class RelCopyExecutor {
public:
    RelCopyExecutor(common::CopyDescription& copyDescription, WAL* wal,
        common::TaskScheduler& taskScheduler, catalog::Catalog& catalog,
        storage::NodesStore& nodesStore, storage::RelTable* table, RelsStatistics* relsStatistics);

    common::offset_t copy(processor::ExecutionContext* executionContext);

private:
    enum class RelCopierType : uint8_t { REL_COLUMN_COPIER_AND_LIST_COUNTER, REL_LIST_COPIER };

    std::unique_ptr<DirectedInMemRelData> initializeDirectedInMemRelData(
        common::RelDataDirection direction);
    void countRelListsSizeAndPopulateColumns(processor::ExecutionContext* executionContext);
    void populateRelLists(processor::ExecutionContext* executionContext);

    std::unique_ptr<RelCopier> createRelCopier(RelCopierType relCopierType);

private:
    common::CopyDescription& copyDescription;
    WAL* wal;
    std::string outputDirectory;
    std::unordered_map<std::string, FileBlockInfo> fileBlockInfos;
    common::TaskScheduler& taskScheduler;
    catalog::Catalog& catalog;
    catalog::RelTableSchema* tableSchema;
    uint64_t numTuples;
    RelsStatistics* relsStatistics;
    storage::NodesStore& nodesStore;
    storage::RelTable* table;
    std::unique_ptr<DirectedInMemRelData> fwdRelData;
    std::unique_ptr<DirectedInMemRelData> bwdRelData;
    std::vector<PrimaryKeyIndex*> pkIndexes;
};

} // namespace storage
} // namespace kuzu
