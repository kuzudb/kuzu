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

TermsTableState::TermsTableState(const Transaction* transaction, FTSUpdateVectors& updateVectors,
    FTSInternalTableInfo& tableInfo)
    : termsTableScanState{&updateVectors.idVector, std::vector{&updateVectors.uint64PropVector},
          updateVectors.dataChunkState},
      termsTableUpdateState{tableInfo.dfColumnID, updateVectors.idVector,
          updateVectors.uint64PropVector} {
    termsTableScanState.setToTable(transaction, tableInfo.termsTable, {tableInfo.dfColumnID}, {});
    termsTableUpdateState.logToWAL = false;
}

FTSInsertState::FTSInsertState(main::ClientContext* context, FTSInternalTableInfo& tableInfo)
    : updateVectors{context->getMemoryManager()},
      docTableInsertState{updateVectors.idVector, updateVectors.int64PKVector,
          std::vector<ValueVector*>{&updateVectors.int64PKVector, &updateVectors.uint64PropVector}},
      termsTableState{context->getTransaction(), updateVectors, tableInfo},
      termsTableInsertState{updateVectors.idVector, updateVectors.stringPKVector,
          std::vector<ValueVector*>{&updateVectors.stringPKVector,
              &updateVectors.uint64PropVector}},
      appearsInTableInsertState{updateVectors.srcIDVector, updateVectors.dstIDVector,
          {&updateVectors.idVector, &updateVectors.uint64PropVector}} {
    tableInfo.docTable->initInsertState(context, docTableInsertState);
    tableInfo.termsTable->initInsertState(context, termsTableInsertState);
    tableInfo.appearsInfoTable->initInsertState(context, appearsInTableInsertState);
    docTableInsertState.logToWAL = false;
    termsTableInsertState.logToWAL = false;
    appearsInTableInsertState.logToWAL = false;
}

IndexTableState::IndexTableState(MemoryManager* mm, const Transaction* transaction,
    FTSInternalTableInfo& tableInfo, std::vector<column_id_t> columnIDs, ValueVector& idVector,
    std::shared_ptr<DataChunkState> dataChunkState) {
    for (auto i = 0u; i < columnIDs.size(); i++) {
        stringVectors.push_back(
            std::make_unique<ValueVector>(LogicalType::STRING(), mm, dataChunkState));
        indexVectors.push_back(stringVectors.back().get());
    }
    scanState = std::make_unique<NodeTableScanState>(&idVector, indexVectors, dataChunkState);
    scanState->setToTable(transaction, tableInfo.table, columnIDs, {});
}

FTSDeleteState::FTSDeleteState(MemoryManager* mm, const Transaction* transaction,
    FTSInternalTableInfo& tableInfo, std::vector<column_id_t> columnIDs)
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

FTSUpdateState::FTSUpdateState(main::ClientContext* context, FTSInternalTableInfo& tableInfo,
    std::vector<common::column_id_t> columnIDs, common::column_id_t colIdxWithUpdate)
    : dataChunkState{DataChunkState::getSingleValueDataChunkState()},
      nodeIDVector{LogicalType::INTERNAL_ID(), context->getMemoryManager(), dataChunkState},
      indexTableState{context->getMemoryManager(), context->getTransaction(), tableInfo, columnIDs,
          nodeIDVector, dataChunkState},
      columnIdxWithUpdate{UINT32_MAX} {
    for (auto i = 0u; i < columnIDs.size(); i++) {
        if (columnIDs[i] == colIdxWithUpdate) {
            columnIdxWithUpdate = i;
            break;
        }
    }
}

} // namespace fts_extension
} // namespace kuzu
