#pragma once

#include <numeric>

#include "processor/operator/sink.h"
#include "storage/store/table.h"

namespace kuzu {
namespace storage {
class MemoryManager;
class ChunkedNodeGroup;
} // namespace storage
namespace processor {

struct BatchInsertInfo {
    catalog::TableCatalogEntry* tableEntry;
    bool compressionEnabled;

    std::vector<common::column_id_t> outputDataColumns;
    std::vector<common::column_id_t> warningDataColumns;

    BatchInsertInfo(catalog::TableCatalogEntry* tableEntry, bool compressionEnabled,
        common::column_id_t numOutputDataColumns, common::column_id_t numWarningDataColumns)
        : tableEntry{tableEntry}, compressionEnabled{compressionEnabled},
          outputDataColumns(numOutputDataColumns), warningDataColumns(numWarningDataColumns) {
        std::iota(outputDataColumns.begin(), outputDataColumns.end(), 0);
        std::iota(warningDataColumns.begin(), warningDataColumns.end(), outputDataColumns.size());
    }
    virtual ~BatchInsertInfo() = default;

    BatchInsertInfo(const BatchInsertInfo& other) = delete;

    virtual std::unique_ptr<BatchInsertInfo> copy() const = 0;

    template<class TARGET>
    TARGET* ptrCast() {
        return common::ku_dynamic_cast<TARGET*>(this);
    }
};

struct BatchInsertSharedState {
    std::mutex mtx;
    std::atomic<common::row_idx_t> numRows;

    // Use a separate mutex for numErroredRows to avoid double-locking in local error handlers
    // As access to numErroredRows is independent of access to other shared state
    std::mutex erroredRowMutex;
    std::shared_ptr<common::row_idx_t> numErroredRows;

    storage::Table* table;
    std::shared_ptr<FactorizedTable> fTable;
    storage::WAL* wal;
    storage::MemoryManager* mm;

    BatchInsertSharedState(storage::Table* table, std::shared_ptr<FactorizedTable> fTable,
        storage::WAL* wal, storage::MemoryManager* mm)
        : numRows{0}, numErroredRows(std::make_shared<common::row_idx_t>(0)), table{table},
          fTable{std::move(fTable)}, wal{wal}, mm{mm} {};
    BatchInsertSharedState(const BatchInsertSharedState& other) = delete;

    virtual ~BatchInsertSharedState() = default;

    std::unique_ptr<BatchInsertSharedState> copy() const {
        auto result = std::make_unique<BatchInsertSharedState>(table, fTable, wal, mm);
        result->numRows.store(numRows.load());
        return result;
    }

    void incrementNumRows(common::row_idx_t numRowsToIncrement) {
        numRows.fetch_add(numRowsToIncrement);
    }
    common::row_idx_t getNumRows() const { return numRows.load(); }
    common::row_idx_t getNumErroredRows() {
        common::UniqLock lockGuard{erroredRowMutex};
        return *numErroredRows;
    }
};

struct BatchInsertLocalState {
    std::unique_ptr<storage::ChunkedNodeGroup> chunkedGroup;

    virtual ~BatchInsertLocalState() = default;

    template<class TARGET>
    TARGET* ptrCast() {
        return common::ku_dynamic_cast<TARGET*>(this);
    }
};

class BatchInsert : public Sink {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::BATCH_INSERT;

public:
    BatchInsert(std::unique_ptr<BatchInsertInfo> info,
        std::shared_ptr<BatchInsertSharedState> sharedState,
        std::unique_ptr<ResultSetDescriptor> resultSetDescriptor, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : Sink{std::move(resultSetDescriptor), type_, id, std::move(printInfo)},
          info{std::move(info)}, sharedState{std::move(sharedState)} {}

    ~BatchInsert() override = default;

    std::unique_ptr<PhysicalOperator> clone() override = 0;

    std::shared_ptr<BatchInsertSharedState> getSharedState() const { return sharedState; }

protected:
    std::unique_ptr<BatchInsertInfo> info;
    std::shared_ptr<BatchInsertSharedState> sharedState;
    std::unique_ptr<BatchInsertLocalState> localState;
};

} // namespace processor
} // namespace kuzu
