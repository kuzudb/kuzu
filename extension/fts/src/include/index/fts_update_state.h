#pragma once

#include "fts_internal_table_info.h"
#include "storage/index/index.h"
#include "storage/table/node_table.h"

namespace kuzu {
namespace fts_extension {

struct FTSUpdateVectors {
    storage::MemoryManager* mm;
    std::shared_ptr<common::DataChunkState> dataChunkState;
    common::ValueVector idVector;
    common::ValueVector srcIDVector;
    common::ValueVector dstIDVector;
    common::ValueVector int64PKVector;
    common::ValueVector stringPKVector;
    common::ValueVector uint64PropVector;

    explicit FTSUpdateVectors(storage::MemoryManager* mm);
};

struct TermsTableState {
    storage::NodeTableScanState termsTableScanState;
    storage::NodeTableUpdateState termsTableUpdateState;

    TermsTableState(const transaction::Transaction* transaction, FTSUpdateVectors& updateVectors,
        FTSInternalTableInfo& tableInfo);
};

struct FTSInsertState final : storage::Index::InsertState {
    FTSUpdateVectors updateVectors;

    storage::NodeTableInsertState docTableInsertState;
    TermsTableState termsTableState;
    storage::NodeTableInsertState termsTableInsertState;
    storage::RelTableInsertState appearsInTableInsertState;

    FTSInsertState(main::ClientContext* context, FTSInternalTableInfo& tableInfo);
};

struct IndexTableState {
    std::vector<std::unique_ptr<common::ValueVector>> stringVectors;
    std::vector<common::ValueVector*> indexVectors;
    std::unique_ptr<storage::NodeTableScanState> scanState;

    IndexTableState(storage::MemoryManager* mm, const transaction::Transaction* transaction,
        FTSInternalTableInfo& tableInfo, std::vector<common::column_id_t> columnIDs,
        common::ValueVector& idVector, std::shared_ptr<common::DataChunkState> dataChunkState);
};

struct FTSDeleteState final : storage::Index::DeleteState {
    FTSUpdateVectors updateVectors;

    storage::NodeTableDeleteState docTableDeleteState;
    storage::NodeTableScanState docTableScanState;
    TermsTableState termsTableState;
    storage::NodeTableDeleteState termsTableDeleteState;
    storage::RelTableDeleteState appearsInTableDeleteState;
    IndexTableState indexTableState;

    FTSDeleteState(storage::MemoryManager* mm, const transaction::Transaction* transaction,
        FTSInternalTableInfo& tableInfo, std::vector<common::column_id_t> columnIDs);
};

struct FTSUpdateState final : storage::Index::UpdateState {
    std::shared_ptr<common::DataChunkState> dataChunkState;
    common::ValueVector nodeIDVector;
    IndexTableState indexTableState;
    common::idx_t columnIdxWithUpdate;
    std::unique_ptr<storage::Index::InsertState> ftsInsertState;
    std::unique_ptr<storage::Index::DeleteState> ftsDeleteState;

    FTSUpdateState(main::ClientContext* context, FTSInternalTableInfo& tableInfo,
        std::vector<common::column_id_t> columnIDs, common::column_id_t colIdxWithUpdate);
};

} // namespace fts_extension
} // namespace kuzu
