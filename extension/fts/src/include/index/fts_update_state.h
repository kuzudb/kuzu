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

    FTSInsertState(storage::MemoryManager* mm, transaction::Transaction* transaction,
        FTSInternalTableInfo& tableInfo);
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

<<<<<<< HEAD
    FTSDeleteState(storage::MemoryManager* mm, transaction::Transaction* transaction,
        FTSInternalTableInfo& tableInfo);
=======
    FTSDeleteState(storage::MemoryManager* mm, const transaction::Transaction* transaction,
        FTSInternalTableInfo& tableInfo, std::vector<common::column_id_t> columnIDs);
>>>>>>> 8c3f70b16 (update)
};

} // namespace fts_extension
} // namespace kuzu
