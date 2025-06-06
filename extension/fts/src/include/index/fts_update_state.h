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

TermsTableState::TermsTableState(const transaction::Transaction* transaction,
    FTSUpdateVectors& updateVectors, FTSInternalTableInfo& tableInfo)
    : termsTableScanState{&updateVectors.idVector, std::vector{&updateVectors.uint64PropVector},
          updateVectors.dataChunkState},
      termsTableUpdateState{tableInfo.dfColumnID, updateVectors.idVector,
          updateVectors.uint64PropVector} {
    termsTableScanState.setToTable(transaction, tableInfo.termsTable, {tableInfo.dfColumnID}, {});
}

struct FTSInsertState final : storage::Index::InsertState {
    FTSUpdateVectors updateVectors;

    storage::NodeTableInsertState docTableInsertState;
    TermsTableState termsTableState;
    storage::NodeTableInsertState termsTableInsertState;
    storage::RelTableInsertState appearsInTableInsertState;

    FTSInsertState(storage::MemoryManager* mm, transaction::Transaction* transaction,
        FTSInternalTableInfo& tableInfo);
};

struct FTSDeleteState final : storage::Index::DeleteState {
    FTSUpdateVectors updateVectors;

    storage::NodeTableDeleteState docTableDeleteState;
    TermsTableState termsTableState;
    storage::NodeTableDeleteState termsTableDeleteState;
    storage::RelTableDeleteState appearsInTableDeleteState;

    FTSDeleteState(storage::MemoryManager* mm, transaction::Transaction* transaction,
        FTSInternalTableInfo& tableInfo);
};

} // namespace fts_extension
} // namespace kuzu
