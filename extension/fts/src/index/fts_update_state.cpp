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
}

FTSDeleteState::FTSDeleteState(MemoryManager* mm, Transaction* transaction,
    FTSInternalTableInfo& tableInfo)
    : updateVectors{mm}, docTableDeleteState{updateVectors.idVector, updateVectors.int64PKVector},
      termsTableState{transaction, updateVectors, tableInfo},
      termsTableDeleteState{updateVectors.idVector, updateVectors.stringPKVector},
      appearsInTableDeleteState{updateVectors.srcIDVector, updateVectors.dstIDVector,
          updateVectors.idVector} {}

} // namespace fts_extension
} // namespace kuzu
