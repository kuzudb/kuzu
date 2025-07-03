#pragma once

#include "processor/operator/sink.h"
#include "processor/result/factorized_table.h"
#include "storage/table/table.h"

namespace kuzu {
namespace storage {
class MemoryManager;
class ChunkedNodeGroup;
} // namespace storage
namespace processor {

struct BatchInsertInfo {
    std::string tableName;
    bool compressionEnabled;

    std::vector<common::LogicalType> columnTypes;
    // TODO(Guodong): Try to merge the following 3 fields into 2
    std::vector<common::column_id_t> insertColumnIDs;
    std::vector<common::column_id_t> outputDataColumns;
    std::vector<common::column_id_t> warningDataColumns;

    BatchInsertInfo(std::string tableName, bool compressionEnabled,
        std::vector<common::column_id_t> insertColumnIDs,
        std::vector<common::LogicalType> columnTypes, common::idx_t numWarningDataColumns)
        : tableName{std::move(tableName)}, compressionEnabled{compressionEnabled},
          columnTypes{std::move(columnTypes)}, insertColumnIDs{std::move(insertColumnIDs)} {
        auto i = 0u;
        for (; i < this->columnTypes.size() - numWarningDataColumns; ++i) {
            outputDataColumns.push_back(i);
        }
        for (; i < this->columnTypes.size(); ++i) {
            warningDataColumns.push_back(i);
        }
    }
    BatchInsertInfo(const BatchInsertInfo& other)
        : tableName{other.tableName}, compressionEnabled{other.compressionEnabled},
          columnTypes{copyVector(other.columnTypes)}, insertColumnIDs{other.insertColumnIDs},
          outputDataColumns{other.outputDataColumns}, warningDataColumns{other.warningDataColumns} {
    }
    DELETE_COPY_ASSN(BatchInsertInfo);
    virtual ~BatchInsertInfo() = default;

    virtual std::unique_ptr<BatchInsertInfo> copy() const = 0;

    template<class TARGET>
    TARGET* ptrCast() {
        return common::ku_dynamic_cast<TARGET*>(this);
    }
};

struct KUZU_API BatchInsertSharedState {
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

    template<class TARGET>
    TARGET* ptrCast() {
        return common::ku_dynamic_cast<TARGET*>(this);
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

class KUZU_API BatchInsert : public Sink {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::BATCH_INSERT;

public:
    BatchInsert(std::string tableName, std::unique_ptr<BatchInsertInfo> info,
        std::shared_ptr<BatchInsertSharedState> sharedState, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : Sink{type_, id, std::move(printInfo)}, tableName{std::move(tableName)},
          info{std::move(info)}, sharedState{std::move(sharedState)} {}

    ~BatchInsert() override = default;

    std::unique_ptr<PhysicalOperator> copy() override = 0;

    std::shared_ptr<BatchInsertSharedState> getSharedState() const { return sharedState; }

protected:
    std::string tableName;
    std::unique_ptr<BatchInsertInfo> info;
    std::shared_ptr<BatchInsertSharedState> sharedState;
    std::unique_ptr<BatchInsertLocalState> localState;
};

} // namespace processor
} // namespace kuzu
