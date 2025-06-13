#include "index/fts_update_state.h"

namespace kuzu {
namespace fts_extension {

using namespace kuzu::storage;
using namespace kuzu::common;
using namespace kuzu::transaction;

FTSUpdateVectors::FTSUpdateVectors(MemoryManager* mm)
    : mm{mm}, dataChunkState{DataChunkState::getSingleValueDataChunkState()},
      idVector{LogicalType::INTERNAL_ID(), mm, dataChunkState},
      srcIDVector{LogicalType::INTERNAL_ID(), mm, dataChunkState},
      dstIDVector{LogicalType::INTERNAL_ID(), mm, dataChunkState},
      int64PKVector{LogicalType::INT64(), mm, dataChunkState},
      stringPKVector{LogicalType::STRING(), mm, dataChunkState},
      uint64PropVector{LogicalType::UINT64(), mm, dataChunkState} {}

TermsTableState::TermsTableState(const transaction::Transaction* transaction,
    FTSUpdateVectors& updateVectors, FTSInternalTableInfo& tableInfo)
    : termsTableScanState{&updateVectors.idVector, std::vector{&updateVectors.uint64PropVector},
          updateVectors.dataChunkState},
      termsTableUpdateState{tableInfo.dfColumnID, updateVectors.idVector,
          updateVectors.uint64PropVector} {
    termsTableScanState.setToTable(transaction, tableInfo.termsTable, {tableInfo.dfColumnID}, {});
    termsTableUpdateState.logToWAL = false;
}

FTSInsertState::FTSInsertState(MemoryManager* mm, Transaction* transaction,
    FTSInternalTableInfo& tableInfo)
    : updateVectors{mm},
      docTableInsertState{updateVectors.idVector, updateVectors.int64PKVector,
          std::vector<ValueVector*>{&updateVectors.int64PKVector, &updateVectors.uint64PropVector}},
      termsTableState{transaction, updateVectors, tableInfo},
      termsTableInsertState{updateVectors.idVector, updateVectors.stringPKVector,
          std::vector<ValueVector*>{&updateVectors.stringPKVector,
              &updateVectors.uint64PropVector}},
      appearsInTableInsertState{updateVectors.srcIDVector, updateVectors.dstIDVector,
          {&updateVectors.idVector, &updateVectors.uint64PropVector}} {
    tableInfo.docTable->initInsertState(transaction, docTableInsertState);
    tableInfo.termsTable->initInsertState(transaction, termsTableInsertState);
    tableInfo.appearsInfoTable->initInsertState(transaction, appearsInTableInsertState);
    docTableInsertState.logToWAL = false;
    termsTableInsertState.logToWAL = false;
    appearsInTableInsertState.logToWAL = false;
}

IndexTableState::IndexTableState(MemoryManager* mm, const transaction::Transaction* transaction,
    FTSInternalTableInfo& tableInfo, std::vector<common::column_id_t> columnIDs,
    common::ValueVector& idVector, std::shared_ptr<common::DataChunkState> dataChunkState) {
    for (auto i = 0u; i < columnIDs.size(); i++) {
        stringVectors.push_back(
            std::make_unique<common::ValueVector>(LogicalType::STRING(), mm, dataChunkState));
        indexVectors.push_back(stringVectors.back().get());
    }
    scanState = std::make_unique<NodeTableScanState>(&idVector, indexVectors, dataChunkState);
    scanState->setToTable(transaction, tableInfo.table, columnIDs, {});
}

FTSDeleteState::FTSDeleteState(MemoryManager* mm, const Transaction* transaction,
    FTSInternalTableInfo& tableInfo, std::vector<common::column_id_t> columnIDs)
    : updateVectors{mm}, docTableDeleteState{updateVectors.idVector, updateVectors.int64PKVector},
      docTableScanState{&updateVectors.idVector, std::vector{&updateVectors.uint64PropVector},
          updateVectors.dataChunkState},
      termsTableState{transaction, updateVectors, tableInfo},
      termsTableDeleteState{updateVectors.idVector, updateVectors.stringPKVector},
      appearsInTableDeleteState{updateVectors.srcIDVector, updateVectors.dstIDVector,
          updateVectors.idVector},
      indexTableState{mm, transaction, tableInfo, std::move(columnIDs), updateVectors.idVector,
          updateVectors.dataChunkState} {
    docTableScanState.setToTable(transaction, tableInfo.docTable, {tableInfo.dfColumnID}, {});
    docTableDeleteState.logToWAL = false;
    termsTableDeleteState.logToWAL = false;
    appearsInTableDeleteState.logToWAL = false;
}

} // namespace fts_extension
} // namespace kuzu
