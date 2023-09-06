#include "processor/operator/persistent/copy_rel.h"

#include "processor/result/factorized_table.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

CopyRelSharedState::CopyRelSharedState(table_id_t tableID, RelsStatistics* relsStatistics,
    std::unique_ptr<DirectedInMemRelData> fwdRelData,
    std::unique_ptr<DirectedInMemRelData> bwdRelData, MemoryManager* memoryManager)
    : tableID{tableID}, relsStatistics{relsStatistics}, fwdRelData{std::move(fwdRelData)},
      bwdRelData{std::move(bwdRelData)}, hasLoggedWAL{false}, numRows{0} {
    auto ftTableSchema = std::make_unique<FactorizedTableSchema>();
    ftTableSchema->appendColumn(
        std::make_unique<ColumnSchema>(false /* flat */, 0 /* dataChunkPos */,
            LogicalTypeUtils::getRowLayoutSize(LogicalType{LogicalTypeID::STRING})));
    fTable = std::make_shared<FactorizedTable>(memoryManager, std::move(ftTableSchema));
}

void CopyRelSharedState::logCopyRelWALRecord(WAL* wal) {
    std::unique_lock xLck{mtx};
    if (!hasLoggedWAL) {
        wal->logCopyRelRecord(tableID);
        wal->flushAllPages();
        hasLoggedWAL = true;
    }
}

void CopyRel::initLocalStateInternal(ResultSet* resultSet_, ExecutionContext* context) {
    localState = std::make_unique<CopyRelLocalState>();
    localState->fwdCopyStates.resize(this->info.schema->getNumProperties());
    for (auto i = 0u; i < this->info.schema->getNumProperties(); i++) {
        localState->fwdCopyStates[i] = std::make_unique<PropertyCopyState>();
    }
    localState->bwdCopyStates.resize(this->info.schema->getNumProperties());
    for (auto i = 0u; i < this->info.schema->getNumProperties(); i++) {
        localState->bwdCopyStates[i] = std::make_unique<PropertyCopyState>();
    }
}

void CopyRel::initGlobalStateInternal(ExecutionContext* context) {
    if (!isCopyAllowed()) {
        throw CopyException(ExceptionMessage::notAllowCopyOnNonEmptyTableException());
    }
}

} // namespace processor
} // namespace kuzu
