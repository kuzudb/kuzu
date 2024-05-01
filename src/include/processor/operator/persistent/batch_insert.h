#pragma once

#include "processor/operator/sink.h"
#include "storage/store/table.h"

namespace kuzu {
namespace processor {

struct BatchInsertInfo {
    catalog::TableCatalogEntry* tableEntry;
    bool compressionEnabled;

    BatchInsertInfo(catalog::TableCatalogEntry* tableEntry, bool compressionEnabled)
        : tableEntry{tableEntry}, compressionEnabled{compressionEnabled} {}
    virtual ~BatchInsertInfo() = default;

    BatchInsertInfo(const BatchInsertInfo& other) = delete;

    inline virtual std::unique_ptr<BatchInsertInfo> copy() const = 0;
};

struct BatchInsertSharedState {
    std::mutex mtx;
    std::atomic<common::row_idx_t> numRows;
    storage::Table* table;
    std::shared_ptr<FactorizedTable> fTable;
    storage::WAL* wal;

    BatchInsertSharedState(storage::Table* table, std::shared_ptr<FactorizedTable> fTable,
        storage::WAL* wal)
        : numRows{0}, table{table}, fTable{std::move(fTable)}, wal{wal} {};
    BatchInsertSharedState(const BatchInsertSharedState& other) = delete;

    virtual ~BatchInsertSharedState() = default;

    std::unique_ptr<BatchInsertSharedState> copy() const {
        auto result = std::make_unique<BatchInsertSharedState>(table, fTable, wal);
        result->numRows.store(numRows.load());
        return result;
    }

    inline void incrementNumRows(common::row_idx_t numRowsToIncrement) {
        numRows.fetch_add(numRowsToIncrement);
    }
    inline common::row_idx_t getNumRows() { return numRows.load(); }
    // NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const.
    inline void logBatchInsertWALRecord() { wal->logCopyTableRecord(table->getTableID()); }
    inline void updateNumTuplesForTable() { table->updateNumTuplesByValue(getNumRows()); }
};

struct BatchInsertLocalState {
    std::unique_ptr<storage::ChunkedNodeGroup> nodeGroup;

    virtual ~BatchInsertLocalState() = default;
};

class BatchInsert : public Sink {
public:
    BatchInsert(std::unique_ptr<BatchInsertInfo> info,
        std::shared_ptr<BatchInsertSharedState> sharedState,
        std::unique_ptr<ResultSetDescriptor> resultSetDescriptor, uint32_t id,
        const std::string& paramsString)
        : Sink{std::move(resultSetDescriptor), PhysicalOperatorType::BATCH_INSERT, id,
              paramsString},
          info{std::move(info)}, sharedState{std::move(sharedState)} {}

    ~BatchInsert() override = default;

    std::unique_ptr<PhysicalOperator> clone() override = 0;

    inline std::shared_ptr<BatchInsertSharedState> getSharedState() const { return sharedState; }

protected:
    std::unique_ptr<BatchInsertInfo> info;
    std::shared_ptr<BatchInsertSharedState> sharedState;
    std::unique_ptr<BatchInsertLocalState> localState;
};

} // namespace processor
} // namespace kuzu
