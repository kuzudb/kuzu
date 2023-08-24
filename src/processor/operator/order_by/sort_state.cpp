#include "processor/operator/order_by/sort_state.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

void SortSharedState::init(const OrderByDataInfo& orderByDataInfo) {
    calculatePayloadSchema(orderByDataInfo);
    auto encodedKeyBlockColOffset = 0ul;
    for (auto i = 0u; i < orderByDataInfo.keysPosAndType.size(); ++i) {
        auto& [dataPos, dataType] = orderByDataInfo.keysPosAndType[i];
        if (PhysicalTypeID::STRING == dataType.getPhysicalType()) {
            // If this is a string column, we need to find the factorizedTable offset for this
            // column.
            auto ftColIdx = 0ul;
            for (auto j = 0u; j < orderByDataInfo.payloadsPosAndType.size(); j++) {
                auto [payloadDataPos, _] = orderByDataInfo.payloadsPosAndType[j];
                if (payloadDataPos == dataPos) {
                    ftColIdx = j;
                }
            }
            strKeyColsInfo.emplace_back(payloadSchema->getColOffset(ftColIdx),
                encodedKeyBlockColOffset, orderByDataInfo.isAscOrder[i]);
        }
        encodedKeyBlockColOffset += OrderByKeyEncoder::getEncodingSize(dataType);
    }
    numBytesPerTuple = encodedKeyBlockColOffset + OrderByConstants::NUM_BYTES_FOR_PAYLOAD_IDX;
}

LocalPayloadTableInfo SortSharedState::getLocalPayloadTable(storage::MemoryManager& memoryManager) {
    std::unique_lock lck{mtx};
    auto payloadTable = std::make_unique<FactorizedTable>(&memoryManager, payloadSchema->copy());
    auto payloadTableInfo = LocalPayloadTableInfo{nextFactorizedTableIdx++, payloadTable.get()};
    payloadTables.push_back(std::move(payloadTable));
    return payloadTableInfo;
}

void SortSharedState::appendLocalSortedKeyBlock(std::shared_ptr<MergedKeyBlocks> mergedDataBlocks) {
    std::unique_lock lck{mtx};
    sortedKeyBlocks->emplace(mergedDataBlocks);
}

void SortSharedState::combineFTHasNoNullGuarantee() {
    for (auto i = 1u; i < payloadTables.size(); i++) {
        payloadTables[0]->mergeMayContainNulls(*payloadTables[i]);
    }
}

std::vector<FactorizedTable*> SortSharedState::getPayloadTables() const {
    std::vector<FactorizedTable*> payloadTablesToReturn;
    payloadTablesToReturn.reserve(payloadTables.size());
    for (auto& payloadTable : payloadTables) {
        payloadTablesToReturn.push_back(payloadTable.get());
    }
    return payloadTablesToReturn;
}

void SortSharedState::calculatePayloadSchema(
    const kuzu::processor::OrderByDataInfo& orderByDataInfo) {
    // The orderByKeyEncoder requires that the orderByKey columns are flat in the
    // factorizedTable. If there is only one unflat dataChunk, we need to flatten the payload
    // columns in factorizedTable because the payload and key columns are in the same
    // dataChunk.
    payloadSchema = std::make_unique<FactorizedTableSchema>();
    for (auto i = 0u; i < orderByDataInfo.payloadsPosAndType.size(); ++i) {
        auto [dataPos, dataType] = orderByDataInfo.payloadsPosAndType[i];
        bool isUnflat = !orderByDataInfo.isPayloadFlat[i] && !orderByDataInfo.mayContainUnflatKey;
        payloadSchema->appendColumn(std::make_unique<ColumnSchema>(isUnflat, dataPos.dataChunkPos,
            isUnflat ? (uint32_t)sizeof(overflow_value_t) :
                       LogicalTypeUtils::getRowLayoutSize(dataType)));
    }
}

void SortLocalState::init(const OrderByDataInfo& orderByDataInfo, SortSharedState& sharedState,
    storage::MemoryManager* memoryManager) {
    localPayloadTableInfo = sharedState.getLocalPayloadTable(*memoryManager);
    orderByKeyEncoder = std::make_unique<OrderByKeyEncoder>(orderByDataInfo, memoryManager,
        localPayloadTableInfo.globalIdx, localPayloadTableInfo.payloadTable->getNumTuplesPerBlock(),
        sharedState.getNumBytesPerTuple());
    radixSorter = std::make_unique<RadixSort>(memoryManager, *localPayloadTableInfo.payloadTable,
        *orderByKeyEncoder, sharedState.getStrKeyColInfo());
}

void SortLocalState::append(std::vector<common::ValueVector*> keyVectors,
    std::vector<common::ValueVector*> payloadVectors) {
    orderByKeyEncoder->encodeKeys(std::move(keyVectors));
    localPayloadTableInfo.payloadTable->append(std::move(payloadVectors));
}

void SortLocalState::finalize(kuzu::processor::SortSharedState& sharedState) {
    for (auto& keyBlock : orderByKeyEncoder->getKeyBlocks()) {
        if (keyBlock->numTuples > 0) {
            radixSorter->sortSingleKeyBlock(*keyBlock);
            sharedState.appendLocalSortedKeyBlock(
                make_shared<MergedKeyBlocks>(orderByKeyEncoder->getNumBytesPerTuple(), keyBlock));
        }
    }
    orderByKeyEncoder->clear();
}

PayloadScanner::PayloadScanner(MergedKeyBlocks* keyBlockToScan,
    std::vector<FactorizedTable*> payloadTables, uint64_t skipNumber, uint64_t limitNumber)
    : keyBlockToScan{std::move(keyBlockToScan)}, payloadTables{std::move(payloadTables)} {
    if (this->keyBlockToScan == nullptr || this->keyBlockToScan->getNumTuples() == 0) {
        nextTupleIdxToReadInMergedKeyBlock = 0;
        endTuplesIdxToReadInMergedKeyBlock = 0;
        return;
    }
    payloadIdxOffset =
        this->keyBlockToScan->getNumBytesPerTuple() - OrderByConstants::NUM_BYTES_FOR_PAYLOAD_IDX;
    colsToScan = std::vector<uint32_t>(this->payloadTables[0]->getTableSchema()->getNumColumns());
    iota(colsToScan.begin(), colsToScan.end(), 0);
    scanSingleTuple = this->payloadTables[0]->hasUnflatCol();
    if (!scanSingleTuple) {
        tuplesToRead = std::make_unique<uint8_t*[]>(DEFAULT_VECTOR_CAPACITY);
    }
    nextTupleIdxToReadInMergedKeyBlock = skipNumber == UINT64_MAX ? 0 : skipNumber;
    endTuplesIdxToReadInMergedKeyBlock =
        limitNumber == UINT64_MAX ? this->keyBlockToScan->getNumTuples() :
                                    std::min(nextTupleIdxToReadInMergedKeyBlock + limitNumber,
                                        this->keyBlockToScan->getNumTuples());
    blockPtrInfo = std::make_unique<BlockPtrInfo>(nextTupleIdxToReadInMergedKeyBlock,
        endTuplesIdxToReadInMergedKeyBlock, this->keyBlockToScan);
}

uint64_t PayloadScanner::scan(std::vector<common::ValueVector*> vectorsToRead) {
    if (nextTupleIdxToReadInMergedKeyBlock >= endTuplesIdxToReadInMergedKeyBlock) {
        return 0;
    } else {
        // If there is an unflat col in factorizedTable, we can only read one
        // tuple at a time. Otherwise, we can read min(DEFAULT_VECTOR_CAPACITY,
        // numTuplesRemainingInMemBlock) tuples.
        if (scanSingleTuple) {
            auto payloadInfo = blockPtrInfo->curTuplePtr + payloadIdxOffset;
            auto blockIdx = OrderByKeyEncoder::getEncodedFTBlockIdx(payloadInfo);
            auto blockOffset = OrderByKeyEncoder::getEncodedFTBlockOffset(payloadInfo);
            auto payloadTable = payloadTables[OrderByKeyEncoder::getEncodedFTIdx(payloadInfo)];
            payloadTable->scan(vectorsToRead,
                blockIdx * payloadTable->getNumTuplesPerBlock() + blockOffset, 1 /* numTuples */);
            blockPtrInfo->curTuplePtr += keyBlockToScan->getNumBytesPerTuple();
            blockPtrInfo->updateTuplePtrIfNecessary();
            nextTupleIdxToReadInMergedKeyBlock++;
            return 1;
        } else {
            auto numTuplesToRead = std::min(DEFAULT_VECTOR_CAPACITY,
                endTuplesIdxToReadInMergedKeyBlock - nextTupleIdxToReadInMergedKeyBlock);
            auto numTuplesRead = 0;
            while (numTuplesRead < numTuplesToRead) {
                auto numTuplesToReadInCurBlock = std::min(
                    numTuplesToRead - numTuplesRead, blockPtrInfo->getNumTuplesLeftInCurBlock());
                for (auto i = 0u; i < numTuplesToReadInCurBlock; i++) {
                    auto payloadInfo = blockPtrInfo->curTuplePtr + payloadIdxOffset;
                    auto blockIdx = OrderByKeyEncoder::getEncodedFTBlockIdx(payloadInfo);
                    auto blockOffset = OrderByKeyEncoder::getEncodedFTBlockOffset(payloadInfo);
                    auto ft = payloadTables[OrderByKeyEncoder::getEncodedFTIdx(payloadInfo)];
                    tuplesToRead[numTuplesRead + i] =
                        ft->getTuple(blockIdx * ft->getNumTuplesPerBlock() + blockOffset);
                    blockPtrInfo->curTuplePtr += keyBlockToScan->getNumBytesPerTuple();
                }
                blockPtrInfo->updateTuplePtrIfNecessary();
                numTuplesRead += numTuplesToReadInCurBlock;
            }
            // TODO(Ziyi): This is a hacky way of using factorizedTable::lookup function,
            // since the tuples in tuplesToRead may not belong to factorizedTable0. The
            // lookup function doesn't perform a check on whether it holds all the tuples in
            // tuplesToRead. We should optimize this lookup function in the orderByScan
            // optimization PR.
            payloadTables[0]->lookup(
                vectorsToRead, colsToScan, tuplesToRead.get(), 0, numTuplesToRead);
            nextTupleIdxToReadInMergedKeyBlock += numTuplesToRead;
            return numTuplesRead;
        }
    }
}

} // namespace processor
} // namespace kuzu
