#include "storage/storage_structure/var_sized_node_column.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

void VarSizedNodeColumnFunc::writeStringValuesToPage(
    uint8_t* frame, uint16_t posInFrame, ValueVector* vector, uint32_t posInVector) {
    auto kuStrInFrame = (ku_string_t*)(frame + (posInFrame * sizeof(ku_string_t)));
    auto kuStrInVector = vector->getValue<ku_string_t>(posInVector);
    if (kuStrInVector.len > ku_string_t::SHORT_STR_LENGTH) {
        throw NotImplementedException("VarSizedNodeColumnFunc::writeStringValuesToPage");
    }
    memcpy(kuStrInFrame->prefix, kuStrInVector.prefix, kuStrInVector.len);
    kuStrInFrame->len = kuStrInVector.len;
}

VarSizedNodeColumn::VarSizedNodeColumn(LogicalType dataType,
    const MetaDiskArrayHeaderInfo& metaDAHeaderInfo, BMFileHandle* nodeGroupsDataFH,
    BMFileHandle* nodeGroupsMetaFH, BufferManager* bufferManager, WAL* wal)
    : NodeColumn{std::move(dataType), metaDAHeaderInfo, nodeGroupsDataFH, nodeGroupsMetaFH,
          bufferManager, wal, true} {
    ovfPageIdxInChunk = ColumnChunk::getNumPagesForBytes(
        numBytesPerFixedSizedValue << StorageConstants::NODE_GROUP_SIZE_LOG2);
    if (this->dataType.getLogicalTypeID() == LogicalTypeID::STRING) {
        writeNodeColumnFunc = VarSizedNodeColumnFunc::writeStringValuesToPage;
    }
}

void VarSizedNodeColumn::scanInternal(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    auto startNodeOffset = nodeIDVector->readNodeOffset(0);
    assert(startNodeOffset % DEFAULT_VECTOR_CAPACITY == 0);
    auto nodeGroupIdx = getNodeGroupIdxFromNodeOffset(startNodeOffset);
    auto chunkStartPageIdx = columnChunksMetaDA->get(nodeGroupIdx, transaction->getType()).pageIdx;
    NodeColumn::scanInternal(transaction, nodeIDVector, resultVector);
    switch (dataType.getLogicalTypeID()) {
    case LogicalTypeID::BLOB:
    case LogicalTypeID::STRING: {
        for (auto i = 0u; i < resultVector->state->selVector->selectedSize; i++) {
            auto pos = resultVector->state->selVector->selectedPositions[i];
            if (resultVector->isNull(pos)) {
                continue;
            }
            readStringValueFromOvf(transaction, resultVector->getValue<ku_string_t>(pos),
                resultVector, chunkStartPageIdx);
        }
    } break;
    case LogicalTypeID::VAR_LIST: {
        for (auto i = 0u; i < resultVector->state->selVector->selectedSize; i++) {
            auto pos = resultVector->state->selVector->selectedPositions[i];
            if (resultVector->isNull(pos)) {
                continue;
            }
            readListValueFromOvf(transaction, resultVector->getValue<ku_list_t>(pos), resultVector,
                pos, chunkStartPageIdx);
        }
    } break;
    default: {
        throw NotImplementedException("VarSizedNodeColumn::scanInternal");
    }
    }
}

void VarSizedNodeColumn::lookupInternal(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    auto startNodeOffset = nodeIDVector->readNodeOffset(0);
    auto nodeGroupIdx = getNodeGroupIdxFromNodeOffset(startNodeOffset);
    auto chunkStartPageIdx = columnChunksMetaDA->get(nodeGroupIdx, transaction->getType()).pageIdx;
    NodeColumn::lookupInternal(transaction, nodeIDVector, resultVector);
    auto pos = resultVector->state->selVector->selectedPositions[0];
    switch (dataType.getLogicalTypeID()) {
    case LogicalTypeID::STRING: {
        if (!resultVector->isNull(pos)) {
            readStringValueFromOvf(transaction, resultVector->getValue<ku_string_t>(pos),
                resultVector, chunkStartPageIdx);
        }
    } break;
    case LogicalTypeID::VAR_LIST: {
        if (!resultVector->isNull(pos)) {
            readListValueFromOvf(transaction, resultVector->getValue<ku_list_t>(pos), resultVector,
                pos, chunkStartPageIdx);
        }
    } break;
    default: {
        throw NotImplementedException("VarSizedNodeColumn::lookupInternal");
    }
    }
}

void VarSizedNodeColumn::readStringValueFromOvf(Transaction* transaction, ku_string_t& kuStr,
    ValueVector* resultVector, page_idx_t chunkStartPageIdx) {
    if (ku_string_t::isShortString(kuStr.len)) {
        return;
    }
    PageByteCursor cursor;
    TypeUtils::decodeOverflowPtr(kuStr.overflowPtr, cursor.pageIdx, cursor.offsetInPage);
    cursor.pageIdx += (ovfPageIdxInChunk + chunkStartPageIdx);
    auto [fileHandleToPin, pageIdxToPin] =
        StorageStructureUtils::getFileHandleAndPhysicalPageIdxToPin(
            *nodeGroupsDataFH, cursor.pageIdx, *wal, transaction->getType());
    bufferManager->optimisticRead(*fileHandleToPin, pageIdxToPin, [&](uint8_t* frame) {
        StringVector::addString(
            resultVector, kuStr, (const char*)(frame + cursor.offsetInPage), kuStr.len);
    });
}

void VarSizedNodeColumn::readListValueFromOvf(Transaction* transaction, ku_list_t kuList,
    ValueVector* resultVector, uint64_t posInVector, page_idx_t chunkStartPageIdx) {
    auto listEntry = ListVector::addList(resultVector, kuList.size);
    resultVector->setValue(posInVector, listEntry);
    PageByteCursor cursor;
    TypeUtils::decodeOverflowPtr(kuList.overflowPtr, cursor.pageIdx, cursor.offsetInPage);
    cursor.pageIdx += (ovfPageIdxInChunk + chunkStartPageIdx);
    auto [fileHandleToPin, pageIdxToPin] =
        StorageStructureUtils::getFileHandleAndPhysicalPageIdxToPin(
            *nodeGroupsDataFH, cursor.pageIdx, *wal, transaction->getType());
    auto dataVector = ListVector::getDataVector(resultVector);
    if (VarListType::getChildType(&resultVector->dataType)->getLogicalTypeID() ==
        LogicalTypeID::VAR_LIST) {
        bufferManager->optimisticRead(*fileHandleToPin, pageIdxToPin, [&](uint8_t* frame) {
            for (auto i = 0u; i < kuList.size; i++) {
                readListValueFromOvf(transaction, ((ku_list_t*)(frame + cursor.offsetInPage))[i],
                    dataVector, listEntry.offset + i, chunkStartPageIdx);
            }
        });
    } else {
        auto bufferToCopy = ListVector::getListValues(resultVector, listEntry);
        bufferManager->optimisticRead(*fileHandleToPin, pageIdxToPin, [&](uint8_t* frame) {
            memcpy(bufferToCopy, frame + cursor.offsetInPage,
                dataVector->getNumBytesPerValue() * kuList.size);
        });
        if (dataVector->dataType.getLogicalTypeID() == LogicalTypeID::STRING) {
            auto kuStrings = (ku_string_t*)bufferToCopy;
            for (auto i = 0u; i < kuList.size; i++) {
                readStringValueFromOvf(transaction, kuStrings[i], dataVector, chunkStartPageIdx);
            }
        }
    }
}

} // namespace storage
} // namespace kuzu
