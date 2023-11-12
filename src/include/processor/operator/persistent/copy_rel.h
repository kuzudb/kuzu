#pragma once

#include "catalog/rel_table_schema.h"
#include "common/enums/rel_direction.h"
#include "processor/operator/partitioner.h"
#include "processor/operator/sink.h"
#include "storage/stats/rels_store_statistics.h"
#include "storage/store/node_group.h"
#include "storage/store/rel_table.h"
#include "storage/wal/wal.h"

namespace kuzu {
namespace processor {

struct CopyRelInfo {
    catalog::RelTableSchema* schema;
    common::vector_idx_t partitioningIdx;
    common::RelDataDirection dataDirection;
    common::ColumnDataFormat dataFormat;

    storage::WAL* wal;
    bool compressionEnabled;

    CopyRelInfo(catalog::RelTableSchema* schema, common::vector_idx_t partitioningIdx,
        common::RelDataDirection dataDirection, common::ColumnDataFormat dataFormat,
        storage::WAL* wal, bool compressionEnabled)
        : schema{schema}, partitioningIdx{partitioningIdx}, dataDirection{dataDirection},
          dataFormat{dataFormat}, wal{wal}, compressionEnabled{compressionEnabled} {}
    CopyRelInfo(const CopyRelInfo& other)
        : schema{other.schema}, partitioningIdx{other.partitioningIdx},
          dataDirection{other.dataDirection}, dataFormat{other.dataFormat}, wal{other.wal} {}

    inline std::unique_ptr<CopyRelInfo> copy() { return std::make_unique<CopyRelInfo>(*this); }
};

class CopyRel;
class CopyRelSharedState {
    friend class CopyRel;

public:
    CopyRelSharedState(common::table_id_t tableID, storage::RelTable* table,
        std::vector<std::unique_ptr<common::LogicalType>> columnTypes,
        storage::RelsStoreStats* relsStatistics, storage::MemoryManager* memoryManager);

    void logCopyRelWALRecord(storage::WAL* wal);
    inline void incrementNumRows(common::row_idx_t numRowsToIncrement) {
        numRows.fetch_add(numRowsToIncrement);
    }
    inline void updateRelsStatistics() { relsStatistics->setNumTuplesForTable(tableID, numRows); }

    inline std::shared_ptr<FactorizedTable> getFTable() { return fTable; }

private:
    common::table_id_t tableID;
    storage::RelTable* table;
    std::vector<std::unique_ptr<common::LogicalType>> columnTypes;
    storage::RelsStoreStats* relsStatistics;
    std::shared_ptr<FactorizedTable> fTable;
    std::atomic<common::row_idx_t> numRows;
};

struct CopyRelLocalState {
    common::partition_idx_t currentPartition = common::INVALID_PARTITION_IDX;
    std::unique_ptr<storage::NodeGroup> nodeGroup;
};

class CopyRel : public Sink {
public:
    CopyRel(std::unique_ptr<CopyRelInfo> info,
        std::shared_ptr<PartitionerSharedState> partitionerSharedState,
        std::shared_ptr<CopyRelSharedState> sharedState,
        std::unique_ptr<ResultSetDescriptor> resultSetDescriptor, uint32_t id,
        const std::string& paramsString)
        : Sink{std::move(resultSetDescriptor), PhysicalOperatorType::COPY_REL, id, paramsString},
          info{std::move(info)}, partitionerSharedState{std::move(partitionerSharedState)},
          sharedState{std::move(sharedState)} {}

    inline bool isSource() const final { return true; }

    void initLocalStateInternal(ResultSet* resultSet_, ExecutionContext* context) final;
    void initGlobalStateInternal(ExecutionContext* context) final;

    void executeInternal(ExecutionContext* context) final;
    void finalize(ExecutionContext* context) final;

    inline std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<CopyRel>(info->copy(), partitionerSharedState, sharedState,
            resultSetDescriptor->copy(), id, paramsString);
    }

private:
    inline bool isCopyAllowed() const {
        return sharedState->relsStatistics
                   ->getRelStatistics(
                       info->schema->tableID, transaction::Transaction::getDummyReadOnlyTrx().get())
                   ->getNextRelOffset() == 0;
    }

    static void populateCSROffsets(storage::ColumnChunk* csrOffsetChunk,
        data_partition_t* partition, common::vector_idx_t offsetVectorIdx);
    static void setOffsetToWithinNodeGroup(
        common::ValueVector* vector, common::offset_t startOffset);
    static void setOffsetFromCSROffsets(
        common::ValueVector* offsetVector, common::offset_t* csrOffsets);

protected:
    std::unique_ptr<CopyRelInfo> info;
    std::shared_ptr<PartitionerSharedState> partitionerSharedState;
    std::shared_ptr<CopyRelSharedState> sharedState;
    std::unique_ptr<CopyRelLocalState> localState;
};

} // namespace processor
} // namespace kuzu
