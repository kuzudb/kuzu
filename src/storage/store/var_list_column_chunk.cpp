#include "storage/store/var_list_column_chunk.h"

#include "common/types/value/value.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

void VarListDataColumnChunk::reset() const {
    dataColumnChunk->resetToEmpty();
}

void VarListDataColumnChunk::resizeBuffer(uint64_t numValues) {
    if (numValues <= capacity) {
        return;
    }
    capacity = capacity == 0 ? 1 : capacity;
    while (capacity < numValues) {
        capacity *= VAR_LIST_RESIZE_RATIO;
    }
    dataColumnChunk->resize(capacity);
}

VarListColumnChunk::VarListColumnChunk(
    LogicalType dataType, uint64_t capacity, bool enableCompression)
    : ColumnChunk{std::move(dataType), capacity, enableCompression, true /* hasNullChunk */},
      enableCompression{enableCompression} {
    varListDataColumnChunk = std::make_unique<VarListDataColumnChunk>(
        ColumnChunkFactory::createColumnChunk(*VarListType::getChildType(&this->dataType),
            enableCompression, false /* needFinalize */, 0 /* capacity */));
    KU_ASSERT(this->dataType.getPhysicalType() == PhysicalTypeID::VAR_LIST);
}

void VarListColumnChunk::append(ColumnChunk* other, offset_t startPosInOtherChunk,
    offset_t startPosInChunk, uint32_t numValuesToAppend) {
    nullChunk->append(
        other->getNullChunk(), startPosInOtherChunk, startPosInChunk, numValuesToAppend);
    auto otherListChunk = reinterpret_cast<VarListColumnChunk*>(other);
    auto offsetInDataChunkToAppend = varListDataColumnChunk->getNumValues();
    for (auto i = 0u; i < numValuesToAppend; i++) {
        offsetInDataChunkToAppend += otherListChunk->getListLen(startPosInOtherChunk + i);
        setValue(offsetInDataChunkToAppend, startPosInChunk + i);
    }
    auto startOffset = otherListChunk->getListOffset(startPosInOtherChunk);
    auto endOffset = otherListChunk->getListOffset(startPosInOtherChunk + numValuesToAppend);
    varListDataColumnChunk->resizeBuffer(offsetInDataChunkToAppend);
    varListDataColumnChunk->dataColumnChunk->append(
        otherListChunk->varListDataColumnChunk->dataColumnChunk.get(), startOffset,
        varListDataColumnChunk->getNumValues(), endOffset - startOffset);
    numValues += numValuesToAppend;
}

void VarListColumnChunk::resetToEmpty() {
    ColumnChunk::resetToEmpty();
    varListDataColumnChunk.reset();
}

void VarListColumnChunk::append(ValueVector* vector, offset_t startPosInChunk) {
    auto nextListOffsetInChunk = getListOffset(startPosInChunk);
    auto offsetBufferToWrite = (offset_t*)(buffer.get());
    for (auto i = 0u; i < vector->state->selVector->selectedSize; i++) {
        auto pos = vector->state->selVector->selectedPositions[i];
        uint64_t listLen = vector->isNull(pos) ? 0 : vector->getValue<list_entry_t>(pos).size;
        nullChunk->setNull(startPosInChunk + i, vector->isNull(pos));
        nextListOffsetInChunk += listLen;
        offsetBufferToWrite[startPosInChunk + i] = nextListOffsetInChunk;
    }
    varListDataColumnChunk->resizeBuffer(nextListOffsetInChunk);
    auto dataVector = ListVector::getDataVector(vector);
    dataVector->setState(std::make_unique<DataChunkState>());
    // TODO(Guodong/Ziyi): Is sel_t enough for list?
    dataVector->state->selVector->resetSelectorToValuePosBuffer();
    for (auto i = 0u; i < vector->state->selVector->selectedSize; i++) {
        auto pos = vector->state->selVector->selectedPositions[i];
        if (vector->isNull(pos)) {
            continue;
        }
        copyListValues(vector->getValue<list_entry_t>(pos), dataVector);
    }
    numValues += vector->state->selVector->selectedSize;
}

void VarListColumnChunk::copyListValues(const list_entry_t& entry, ValueVector* dataVector) {
    auto numListValuesToCopy = entry.size;
    auto numListValuesCopied = 0;
    while (numListValuesCopied < numListValuesToCopy) {
        auto numListValuesToCopyInBatch =
            std::min<uint64_t>(numListValuesToCopy - numListValuesCopied, DEFAULT_VECTOR_CAPACITY);
        dataVector->state->selVector->selectedSize = numListValuesToCopyInBatch;
        for (auto j = 0u; j < numListValuesToCopyInBatch; j++) {
            dataVector->state->selVector->selectedPositions[j] =
                entry.offset + numListValuesCopied + j;
        }
        varListDataColumnChunk->append(dataVector);
        numListValuesCopied += numListValuesToCopyInBatch;
    }
}

void AuxVarListColumnChunk::write(ValueVector* vector, ValueVector* offsetInChunkVector) {
    KU_ASSERT(vector->dataType.getPhysicalType() == dataType.getPhysicalType() &&
              offsetInChunkVector->dataType.getPhysicalType() == PhysicalTypeID::INT64 &&
              vector->state->selVector->selectedSize ==
                  offsetInChunkVector->state->selVector->selectedSize);
    auto offsets = (offset_t*)offsetInChunkVector->getData();
    auto chunkListEntries = (list_entry_t*)buffer.get();
    for (auto i = 0u; i < offsetInChunkVector->state->selVector->selectedSize; i++) {
        auto offsetInChunk = offsets[offsetInChunkVector->state->selVector->selectedPositions[i]];
        KU_ASSERT(offsetInChunk < capacity);
        auto offsetInVector = vector->state->selVector->selectedPositions[i];
        uint64_t listLen = vector->isNull(offsetInVector) ?
                               0 :
                               vector->getValue<list_entry_t>(offsetInVector).size;
        chunkListEntries[offsetInChunk].offset = lastDataOffset;
        chunkListEntries[offsetInChunk].size = listLen;
        lastDataOffset += listLen;
        nullChunk->setNull(offsetInChunk, vector->isNull(offsetInVector));
        if (offsetInChunk >= numValues) {
            numValues = offsetInChunk + 1;
        }
    }
    varListDataColumnChunk->resizeBuffer(lastDataOffset);
    auto dataVector = ListVector::getDataVector(vector);
    dataVector->setState(std::make_unique<DataChunkState>());
    dataVector->state->selVector->resetSelectorToValuePosBuffer();
    for (auto i = 0u; i < vector->state->selVector->selectedSize; i++) {
        auto pos = vector->state->selVector->selectedPositions[i];
        if (vector->isNull(pos)) {
            continue;
        }
        copyListValues(vector->getValue<list_entry_t>(pos), dataVector);
    }
}

void AuxVarListColumnChunk::resize(uint64_t newCapacity) {
    // AuxVarListColumnChunk requires twice the size of the capacity of the VarListColumnChunk.
    ColumnChunk::resize(newCapacity * 2);
}

std::unique_ptr<ColumnChunk> AuxVarListColumnChunk::finalize() {
    std::unique_ptr<ColumnChunk> result = ColumnChunkFactory::createColumnChunk(
        dataType, enableCompression, false /* needFinalize */, capacity / 2);
    KU_ASSERT(result->getDataType().getPhysicalType() == PhysicalTypeID::VAR_LIST);
    auto resultVarListChunk = reinterpret_cast<VarListColumnChunk*>(result.get());
    resultVarListChunk->getNullChunk()->append(nullChunk.get(), 0, 0, numValues);
    auto resultOffsets = reinterpret_cast<offset_t*>(resultVarListChunk->getData());
    resultVarListChunk->getVarListDataColumnChunk()->resizeBuffer(lastDataOffset);
    auto listEntries = (list_entry_t*)buffer.get();
    auto offsetInResultDataChunk = 0;
    for (auto i = 0u; i < numValues; i++) {
        if (listEntries[i].size > 0) {
            resultVarListChunk->getDataColumnChunk()->append(
                varListDataColumnChunk->dataColumnChunk.get(), listEntries[i].offset,
                offsetInResultDataChunk, listEntries[i].size);
        }
        offsetInResultDataChunk += listEntries[i].size;
        resultOffsets[i] = offsetInResultDataChunk;
    }
    resultVarListChunk->setNumValues(numValues);
    return result;
}

} // namespace storage
} // namespace kuzu
