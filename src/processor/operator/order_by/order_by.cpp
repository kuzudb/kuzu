#include "processor/operator/order_by/order_by.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

void OrderBy::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    for (auto [dataPos, _] : orderByDataInfo.payloadsPosAndType) {
        auto vector = resultSet->getValueVector(dataPos);
        vectorsToAppend.push_back(vector.get());
    }
    // TODO(Ziyi): this is implemented differently from other sink operators. Normally we append
    // local table to global at the end of the execution. But here your encoder seem to need encode
    // tableIdx which closely associated with the execution order of thread. We prefer a unified
    // design pattern for sink operators.
    localFactorizedTable =
        make_shared<FactorizedTable>(context->memoryManager, populateTableSchema());
    factorizedTableIdx = sharedState->getNextFactorizedTableIdx();
    sharedState->appendFactorizedTable(factorizedTableIdx, localFactorizedTable);
    for (auto [dataPos, _] : orderByDataInfo.keysPosAndType) {
        keyVectors.push_back(resultSet->getValueVector(dataPos).get());
    }
    orderByKeyEncoder = std::make_unique<OrderByKeyEncoder>(keyVectors, orderByDataInfo.isAscOrder,
        context->memoryManager, factorizedTableIdx, localFactorizedTable->getNumTuplesPerBlock(),
        sharedState->numBytesPerTuple);
    radixSorter = std::make_unique<RadixSort>(context->memoryManager, *localFactorizedTable,
        *orderByKeyEncoder, sharedState->strKeyColsInfo);
}

std::unique_ptr<FactorizedTableSchema> OrderBy::populateTableSchema() {
    std::unique_ptr<FactorizedTableSchema> tableSchema = std::make_unique<FactorizedTableSchema>();
    // The orderByKeyEncoder requires that the orderByKey columns are flat in the
    // factorizedTable. If there is only one unflat dataChunk, we need to flatten the payload
    // columns in factorizedTable because the payload and key columns are in the same
    // dataChunk.
    for (auto i = 0u; i < orderByDataInfo.payloadsPosAndType.size(); ++i) {
        auto [dataPos, dataType] = orderByDataInfo.payloadsPosAndType[i];
        bool isUnflat = !orderByDataInfo.isPayloadFlat[i] && !orderByDataInfo.mayContainUnflatKey;
        tableSchema->appendColumn(std::make_unique<ColumnSchema>(isUnflat, dataPos.dataChunkPos,
            isUnflat ? (uint32_t)sizeof(overflow_value_t) :
                       LogicalTypeUtils::getRowLayoutSize(dataType)));
    }
    return tableSchema;
}

void OrderBy::initGlobalStateInternal(kuzu::processor::ExecutionContext* context) {
    std::vector<StrKeyColInfo> strKeyColInfo;
    auto encodedKeyBlockColOffset = 0ul;
    auto tableSchema = populateTableSchema();
    for (auto i = 0u; i < orderByDataInfo.keysPosAndType.size(); ++i) {
        auto [dataPos, dataType] = orderByDataInfo.keysPosAndType[i];
        if (PhysicalTypeID::STRING == dataType.getPhysicalType()) {
            // If this is a string column, we need to find the factorizedTable offset for this
            // column.
            auto factorizedTableColIdx = 0ul;
            for (auto j = 0u; j < orderByDataInfo.payloadsPosAndType.size(); j++) {
                auto [payloadDataPos, _] = orderByDataInfo.payloadsPosAndType[j];
                if (payloadDataPos == dataPos) {
                    factorizedTableColIdx = j;
                }
            }
            strKeyColInfo.emplace_back(
                StrKeyColInfo(tableSchema->getColOffset(factorizedTableColIdx),
                    encodedKeyBlockColOffset, orderByDataInfo.isAscOrder[i]));
        }
        encodedKeyBlockColOffset += OrderByKeyEncoder::getEncodingSize(dataType);
    }
    sharedState->setStrKeyColInfo(strKeyColInfo);
    // TODO(Ziyi): comment about +8
    auto numBytesPerTuple = encodedKeyBlockColOffset + 8;
    sharedState->setNumBytesPerTuple(numBytesPerTuple);
}

void OrderBy::executeInternal(ExecutionContext* context) {
    // Append thread-local tuples.
    while (children[0]->getNextTuple(context)) {
        for (auto i = 0u; i < resultSet->multiplicity; i++) {
            orderByKeyEncoder->encodeKeys();
            // The orderByKeyEncoder requires that the orderByKey columns are flat in the
            // factorizedTable. If there is a single dataChunk (unflat or not), it means all the key
            // columns and payload columns are in the same datachunk. Since we need to flatten key
            // columns, we flatten all columns in the factorized table. If there are multiple
            // datachunks, then the datachunks that the keys belong to are guaranteed by the
            // frontend to be flattened (see ProjectionEnumerator), so a column is flat in
            // factorized table if and only if its corresponding vector is flat.
            localFactorizedTable->append(vectorsToAppend);
        }
    }
    for (auto& keyBlock : orderByKeyEncoder->getKeyBlocks()) {
        if (keyBlock->numTuples > 0) {
            radixSorter->sortSingleKeyBlock(*keyBlock);
            sharedState->appendSortedKeyBlock(
                make_shared<MergedKeyBlocks>(orderByKeyEncoder->getNumBytesPerTuple(), keyBlock));
        }
    }
}

} // namespace processor
} // namespace kuzu
