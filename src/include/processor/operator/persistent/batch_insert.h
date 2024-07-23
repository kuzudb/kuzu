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

    virtual std::unique_ptr<BatchInsertInfo> copy() const = 0;

    template<class TARGET>
    TARGET* ptrCast() {
        return common::ku_dynamic_cast<BatchInsertInfo*, TARGET*>(this);
    }
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

    void incrementNumRows(common::row_idx_t numRowsToIncrement) {
        numRows.fetch_add(numRowsToIncrement);
    }
    common::row_idx_t getNumRows() const { return numRows.load(); }
};

struct BatchInsertLocalState {
    std::unique_ptr<storage::ChunkedNodeGroup> chunkedGroup;

    virtual ~BatchInsertLocalState() = default;

    template<class TARGET>
    TARGET* ptrCast() {
        return common::ku_dynamic_cast<BatchInsertLocalState*, TARGET*>(this);
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
