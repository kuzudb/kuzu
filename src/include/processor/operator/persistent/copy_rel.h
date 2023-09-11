#pragma once

#include "catalog/rel_table_schema.h"
#include "processor/data_pos.h"
#include "processor/operator/sink.h"
#include "storage/in_mem_storage_structure/in_mem_column.h"
#include "storage/in_mem_storage_structure/in_mem_column_chunk.h"
#include "storage/in_mem_storage_structure/in_mem_lists.h"
#include "storage/store/rels_statistics.h"

namespace kuzu {
namespace processor {

struct DirectedInMemRelColumns {
    std::unique_ptr<storage::InMemColumnChunk> adjColumnChunk;
    std::unique_ptr<storage::InMemColumn> adjColumn;
    std::unordered_map<common::property_id_t, std::unique_ptr<storage::InMemColumnChunk>>
        propertyColumnChunks;
    std::unordered_map<common::property_id_t, std::unique_ptr<storage::InMemColumn>>
        propertyColumns;
};

struct DirectedInMemRelLists {
    std::unique_ptr<storage::atomic_uint64_vec_t> relListsSizes;
    std::unique_ptr<storage::InMemAdjLists> adjList;
    std::unordered_map<common::property_id_t, std::unique_ptr<storage::InMemLists>> propertyLists;
};

enum class RelDataFormat : uint8_t { COLUMN = 0, CSR_LISTS = 1 };

class DirectedInMemRelData {
public:
    void setColumns(std::unique_ptr<DirectedInMemRelColumns> columns_) {
        relDataFormat = RelDataFormat::COLUMN;
        this->columns = std::move(columns_);
    }

    void setRelLists(std::unique_ptr<DirectedInMemRelLists> lists_) {
        relDataFormat = RelDataFormat::CSR_LISTS;
        this->lists = std::move(lists_);
    }

public:
    RelDataFormat relDataFormat;
    std::unique_ptr<DirectedInMemRelColumns> columns;
    std::unique_ptr<DirectedInMemRelLists> lists;
};

struct CopyRelInfo {
    catalog::RelTableSchema* schema;
    std::vector<DataPos> dataPoses;
    DataPos offsetPos;
    DataPos boundOffsetPos;
    DataPos nbrOffsetPos;
    storage::WAL* wal;
};

class CopyRel;
class CopyRelSharedState {
    friend class CopyRel;
    friend class CopyRelColumns;
    friend class CopyRelLists;

public:
    CopyRelSharedState(common::table_id_t tableID, storage::RelsStatistics* relsStatistics,
        std::unique_ptr<DirectedInMemRelData> fwdRelData,
        std::unique_ptr<DirectedInMemRelData> bwdRelData, storage::MemoryManager* memoryManager);

    void logCopyRelWALRecord(storage::WAL* wal);
    inline void incrementNumRows(common::row_idx_t numRowsToIncrement) {
        numRows.fetch_add(numRowsToIncrement);
    }
    inline void updateRelsStatistics() { relsStatistics->setNumTuplesForTable(tableID, numRows); }

    inline std::shared_ptr<FactorizedTable> getFTable() { return fTable; }

private:
    std::mutex mtx;
    common::table_id_t tableID;
    storage::RelsStatistics* relsStatistics;
    std::unique_ptr<DirectedInMemRelData> fwdRelData;
    std::unique_ptr<DirectedInMemRelData> bwdRelData;
    std::shared_ptr<FactorizedTable> fTable;
    bool hasLoggedWAL;
    std::atomic<common::row_idx_t> numRows;
};

struct CopyRelLocalState {
    std::vector<std::unique_ptr<storage::PropertyCopyState>> fwdCopyStates;
    std::vector<std::unique_ptr<storage::PropertyCopyState>> bwdCopyStates;
};

class CopyRel : public Sink {
public:
    // Copy rel columns.
    CopyRel(CopyRelInfo info, std::shared_ptr<CopyRelSharedState> sharedState,
        std::unique_ptr<ResultSetDescriptor> resultSetDescriptor, PhysicalOperatorType opType,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : Sink{std::move(resultSetDescriptor), opType, std::move(child), id, paramsString},
          info{std::move(info)}, sharedState{std::move(sharedState)} {}

    // Copy rel lists.
    CopyRel(CopyRelInfo info, std::shared_ptr<CopyRelSharedState> sharedState,
        std::unique_ptr<ResultSetDescriptor> resultSetDescriptor, PhysicalOperatorType opType,
        std::unique_ptr<PhysicalOperator> left, std::unique_ptr<PhysicalOperator> right,
        uint32_t id, const std::string& paramsString)
        : Sink{std::move(resultSetDescriptor), opType, std::move(left), id, paramsString},
          info{std::move(info)}, sharedState{std::move(sharedState)} {
        children.push_back(std::move(right));
    }

    void initLocalStateInternal(ResultSet* resultSet_, ExecutionContext* context) final;
    void initGlobalStateInternal(ExecutionContext* context) final;

private:
    inline bool isCopyAllowed() const {
        return sharedState->relsStatistics->getRelStatistics(info.schema->tableID)
                   ->getNextRelOffset() == 0;
    }

protected:
    CopyRelInfo info;
    std::shared_ptr<CopyRelSharedState> sharedState;
    std::unique_ptr<CopyRelLocalState> localState;
};

} // namespace processor
} // namespace kuzu
