#include "common/vector/value_vector.h"

#include "common/null_buffer.h"
#include "common/vector/auxiliary_buffer.h"

namespace kuzu {
namespace common {

ValueVector::ValueVector(LogicalType dataType, storage::MemoryManager* memoryManager)
    : dataType{std::move(dataType)} {
    numBytesPerValue = getDataTypeSize(this->dataType);
    initializeValueBuffer();
    nullMask = std::make_unique<NullMask>();
    auxiliaryBuffer = AuxiliaryBufferFactory::getAuxiliaryBuffer(this->dataType, memoryManager);
}

void ValueVector::setState(std::shared_ptr<DataChunkState> state) {
    this->state = state;
    if (dataType.getLogicalTypeID() == LogicalTypeID::STRUCT) {
        auto childrenVectors = StructVector::getFieldVectors(this);
        for (auto& childVector : childrenVectors) {
            childVector->setState(state);
        }
    }
}

bool NodeIDVector::discardNull(ValueVector& vector) {
    if (vector.hasNoNullsGuarantee()) {
        return true;
    } else {
        auto selectedPos = 0u;
        if (vector.state->selVector->isUnfiltered()) {
            vector.state->selVector->resetSelectorToValuePosBuffer();
            for (auto i = 0u; i < vector.state->selVector->selectedSize; i++) {
                vector.state->selVector->selectedPositions[selectedPos] = i;
                selectedPos += !vector.isNull(i);
            }
        } else {
            for (auto i = 0u; i < vector.state->selVector->selectedSize; i++) {
                auto pos = vector.state->selVector->selectedPositions[i];
                vector.state->selVector->selectedPositions[selectedPos] = pos;
                selectedPos += !vector.isNull(pos);
            }
        }
        vector.state->selVector->selectedSize = selectedPos;
        return selectedPos > 0;
    }
}

template<typename T>
void ValueVector::setValue(uint32_t pos, T val) {
    ((T*)valueBuffer.get())[pos] = val;
}

void ValueVector::copyFromRowData(uint32_t pos, const uint8_t* rowData) {
    switch (dataType.getPhysicalType()) {
    case PhysicalTypeID::STRUCT: {
        StructVector::copyFromRowData(this, pos, rowData);
    } break;
    case PhysicalTypeID::VAR_LIST: {
        ListVector::copyFromRowData(this, pos, rowData);
    } break;
    case PhysicalTypeID::STRING: {
        StringVector::addString(this, pos, *(ku_string_t*)rowData);
    } break;
    default: {
        auto dataTypeSize = LogicalTypeUtils::getRowLayoutSize(dataType);
        memcpy(getData() + pos * dataTypeSize, rowData, dataTypeSize);
    }
    }
}

void ValueVector::copyToRowData(
    uint32_t pos, uint8_t* rowData, InMemOverflowBuffer* rowOverflowBuffer) const {
    switch (dataType.getPhysicalType()) {
    case PhysicalTypeID::STRUCT: {
        StructVector::copyToRowData(this, pos, rowData, rowOverflowBuffer);
    } break;
    case PhysicalTypeID::VAR_LIST: {
        ListVector::copyToRowData(this, pos, rowData, rowOverflowBuffer);
    } break;
    case PhysicalTypeID::STRING: {
        StringVector::copyToRowData(this, pos, rowData, rowOverflowBuffer);
    } break;
    default: {
        auto dataTypeSize = LogicalTypeUtils::getRowLayoutSize(dataType);
        memcpy(rowData, getData() + pos * dataTypeSize, dataTypeSize);
    }
    }
}

void ValueVector::copyFromVectorData(
    uint8_t* dstData, const ValueVector* srcVector, const uint8_t* srcVectorData) {
    assert(srcVector->dataType.getPhysicalType() == dataType.getPhysicalType());
    switch (srcVector->dataType.getPhysicalType()) {
    case PhysicalTypeID::STRUCT: {
        StructVector::copyFromVectorData(this, dstData, srcVector, srcVectorData);
    } break;
    case PhysicalTypeID::VAR_LIST: {
        ListVector::copyFromVectorData(this, dstData, srcVector, srcVectorData);
    } break;
    case PhysicalTypeID::STRING: {
        StringVector::addString(this, *(ku_string_t*)dstData, *(ku_string_t*)srcVectorData);
    } break;
    default: {
        memcpy(dstData, srcVectorData, srcVector->getNumBytesPerValue());
    }
    }
}

void ValueVector::resetAuxiliaryBuffer() {
    switch (dataType.getPhysicalType()) {
    case PhysicalTypeID::STRING: {
        reinterpret_cast<StringAuxiliaryBuffer*>(auxiliaryBuffer.get())->resetOverflowBuffer();
        return;
    }
    case PhysicalTypeID::VAR_LIST: {
        reinterpret_cast<ListAuxiliaryBuffer*>(auxiliaryBuffer.get())->resetSize();
        return;
    }
    case PhysicalTypeID::STRUCT: {
        auto structAuxiliaryBuffer =
            reinterpret_cast<StructAuxiliaryBuffer*>(auxiliaryBuffer.get());
        for (auto& vector : structAuxiliaryBuffer->getFieldVectors()) {
            vector->resetAuxiliaryBuffer();
        }
        return;
    }
    default:
        return;
    }
}

uint32_t ValueVector::getDataTypeSize(const LogicalType& type) {
    switch (type.getPhysicalType()) {
    case PhysicalTypeID::STRING: {
        return sizeof(ku_string_t);
    }
    case PhysicalTypeID::FIXED_LIST: {
        return getDataTypeSize(*FixedListType::getChildType(&type)) *
               FixedListType::getNumElementsInList(&type);
    }
    case PhysicalTypeID::STRUCT: {
        return sizeof(struct_entry_t);
    }
    case PhysicalTypeID::VAR_LIST: {
        return sizeof(list_entry_t);
    }
    case PhysicalTypeID::ARROW_COLUMN: {
        return 0;
    }
    default: {
        return PhysicalTypeUtils::getFixedTypeSize(type.getPhysicalType());
    }
    }
}

void ValueVector::initializeValueBuffer() {
    valueBuffer = std::make_unique<uint8_t[]>(numBytesPerValue * DEFAULT_VECTOR_CAPACITY);
    if (dataType.getPhysicalType() == PhysicalTypeID::STRUCT) {
        // For struct valueVectors, each struct_entry_t stores its current position in the
        // valueVector.
        StructVector::initializeEntries(this);
    }
}

void ArrowColumnVector::setArrowColumn(ValueVector* vector, std::shared_ptr<arrow::Array> column) {
    auto arrowColumnBuffer =
        reinterpret_cast<ArrowColumnAuxiliaryBuffer*>(vector->auxiliaryBuffer.get());
    arrowColumnBuffer->column = std::move(column);
}

template void ValueVector::setValue<nodeID_t>(uint32_t pos, nodeID_t val);
template void ValueVector::setValue<bool>(uint32_t pos, bool val);
template void ValueVector::setValue<int64_t>(uint32_t pos, int64_t val);
template void ValueVector::setValue<hash_t>(uint32_t pos, hash_t val);
template void ValueVector::setValue<double_t>(uint32_t pos, double_t val);
template void ValueVector::setValue<date_t>(uint32_t pos, date_t val);
template void ValueVector::setValue<timestamp_t>(uint32_t pos, timestamp_t val);
template void ValueVector::setValue<interval_t>(uint32_t pos, interval_t val);
template void ValueVector::setValue<list_entry_t>(uint32_t pos, list_entry_t val);

template<>
void ValueVector::setValue(uint32_t pos, ku_string_t val) {
    StringVector::addString(this, pos, val);
}
template<>
void ValueVector::setValue(uint32_t pos, std::string val) {
    StringVector::addString(this, pos, val.data(), val.length());
}

void StringVector::addString(ValueVector* vector, uint32_t vectorPos, ku_string_t& srcStr) {
    assert(vector->dataType.getPhysicalType() == PhysicalTypeID::STRING);
    auto stringBuffer = reinterpret_cast<StringAuxiliaryBuffer*>(vector->auxiliaryBuffer.get());
    auto& dstStr = vector->getValue<ku_string_t>(vectorPos);
    if (ku_string_t::isShortString(srcStr.len)) {
        dstStr.setShortString(srcStr);
    } else {
        dstStr.overflowPtr = reinterpret_cast<uint64_t>(stringBuffer->allocateOverflow(srcStr.len));
        dstStr.setLongString(srcStr);
    }
}

void StringVector::addString(
    ValueVector* vector, uint32_t vectorPos, const char* srcStr, uint64_t length) {
    assert(vector->dataType.getPhysicalType() == PhysicalTypeID::STRING);
    auto stringBuffer = reinterpret_cast<StringAuxiliaryBuffer*>(vector->auxiliaryBuffer.get());
    auto& dstStr = vector->getValue<ku_string_t>(vectorPos);
    if (ku_string_t::isShortString(length)) {
        dstStr.setShortString(srcStr, length);
    } else {
        dstStr.overflowPtr = reinterpret_cast<uint64_t>(stringBuffer->allocateOverflow(length));
        dstStr.setLongString(srcStr, length);
    }
}

void StringVector::addString(ValueVector* vector, ku_string_t& dstStr, ku_string_t& srcStr) {
    assert(vector->dataType.getPhysicalType() == PhysicalTypeID::STRING);
    auto stringBuffer = reinterpret_cast<StringAuxiliaryBuffer*>(vector->auxiliaryBuffer.get());
    if (ku_string_t::isShortString(srcStr.len)) {
        dstStr.setShortString(srcStr);
    } else {
        dstStr.overflowPtr = reinterpret_cast<uint64_t>(stringBuffer->allocateOverflow(srcStr.len));
        dstStr.setLongString(srcStr);
    }
}

void StringVector::addString(
    ValueVector* vector, ku_string_t& dstStr, const char* srcStr, uint64_t length) {
    assert(vector->dataType.getPhysicalType() == PhysicalTypeID::STRING);
    auto stringBuffer = reinterpret_cast<StringAuxiliaryBuffer*>(vector->auxiliaryBuffer.get());
    if (ku_string_t::isShortString(length)) {
        dstStr.setShortString(srcStr, length);
    } else {
        dstStr.overflowPtr = reinterpret_cast<uint64_t>(stringBuffer->allocateOverflow(length));
        dstStr.setLongString(srcStr, length);
    }
}

void StringVector::copyToRowData(const ValueVector* vector, uint32_t pos, uint8_t* rowData,
    InMemOverflowBuffer* rowOverflowBuffer) {
    auto& srcStr = vector->getValue<ku_string_t>(pos);
    auto& dstStr = *(ku_string_t*)rowData;
    if (ku_string_t::isShortString(srcStr.len)) {
        dstStr.setShortString(srcStr);
    } else {
        dstStr.overflowPtr =
            reinterpret_cast<uint64_t>(rowOverflowBuffer->allocateSpace(srcStr.len));
        dstStr.setLongString(srcStr);
    }
}

void ListVector::copyFromRowData(ValueVector* vector, uint32_t pos, const uint8_t* rowData) {
    assert(vector->dataType.getPhysicalType() == PhysicalTypeID::VAR_LIST);
    auto& srcKuList = *(ku_list_t*)rowData;
    auto srcNullBytes = reinterpret_cast<uint8_t*>(srcKuList.overflowPtr);
    auto srcListValues = srcNullBytes + NullBuffer::getNumBytesForNullValues(srcKuList.size);
    auto dstListEntry = addList(vector, srcKuList.size);
    vector->setValue<list_entry_t>(pos, dstListEntry);
    auto resultDataVector = getDataVector(vector);
    auto rowLayoutSize = LogicalTypeUtils::getRowLayoutSize(resultDataVector->dataType);
    for (auto i = 0u; i < srcKuList.size; i++) {
        auto dstListValuePos = dstListEntry.offset + i;
        if (NullBuffer::isNull(srcNullBytes, i)) {
            resultDataVector->setNull(dstListValuePos, true);
        } else {
            resultDataVector->copyFromRowData(dstListValuePos, srcListValues);
        }
        srcListValues += rowLayoutSize;
    }
}

void ListVector::copyToRowData(const ValueVector* vector, uint32_t pos, uint8_t* rowData,
    InMemOverflowBuffer* rowOverflowBuffer) {
    auto& srcListEntry = vector->getValue<list_entry_t>(pos);
    auto srcListDataVector = common::ListVector::getDataVector(vector);
    auto& dstListEntry = *(ku_list_t*)rowData;
    dstListEntry.size = srcListEntry.size;
    auto nullBytesSize = NullBuffer::getNumBytesForNullValues(dstListEntry.size);
    auto dataRowLayoutSize = LogicalTypeUtils::getRowLayoutSize(srcListDataVector->dataType);
    auto dstListOverflowSize = dataRowLayoutSize * dstListEntry.size + nullBytesSize;
    auto dstListOverflow = rowOverflowBuffer->allocateSpace(dstListOverflowSize);
    dstListEntry.overflowPtr = reinterpret_cast<uint64_t>(dstListOverflow);
    NullBuffer::initNullBytes(dstListOverflow, dstListEntry.size);
    auto dstListValues = dstListOverflow + nullBytesSize;
    for (auto i = 0u; i < srcListEntry.size; i++) {
        if (srcListDataVector->isNull(srcListEntry.offset + i)) {
            NullBuffer::setNull(dstListOverflow, i);
        } else {
            srcListDataVector->copyToRowData(
                srcListEntry.offset + i, dstListValues, rowOverflowBuffer);
        }
        dstListValues += dataRowLayoutSize;
    }
}

void ListVector::copyFromVectorData(ValueVector* dstVector, uint8_t* dstData,
    const ValueVector* srcVector, const uint8_t* srcData) {
    auto& srcListEntry = *(common::list_entry_t*)(srcData);
    auto& dstListEntry = *(common::list_entry_t*)(dstData);
    dstListEntry = addList(dstVector, srcListEntry.size);
    auto srcListData = getListValues(srcVector, srcListEntry);
    auto srcDataVector = getDataVector(srcVector);
    auto dstListData = getListValues(dstVector, dstListEntry);
    auto dstDataVector = getDataVector(dstVector);
    auto numBytesPerValue = srcDataVector->getNumBytesPerValue();
    for (auto i = 0u; i < srcListEntry.size; i++) {
        if (srcDataVector->isNull(srcListEntry.offset + i)) {
            dstDataVector->setNull(dstListEntry.offset + i, true);
        } else {
            dstDataVector->copyFromVectorData(dstListData, srcDataVector, srcListData);
        }
        srcListData += numBytesPerValue;
        dstListData += numBytesPerValue;
    }
}

void StructVector::copyFromRowData(ValueVector* vector, uint32_t pos, const uint8_t* rowData) {
    assert(vector->dataType.getPhysicalType() == PhysicalTypeID::STRUCT);
    auto& structFields = getFieldVectors(vector);
    auto structNullBytes = rowData;
    auto structValues = structNullBytes + NullBuffer::getNumBytesForNullValues(structFields.size());
    for (auto i = 0u; i < structFields.size(); i++) {
        auto structField = structFields[i];
        if (NullBuffer::isNull(structNullBytes, i)) {
            structField->setNull(pos, true /* isNull */);
        } else {
            structField->copyFromRowData(pos, structValues);
        }
        structValues += LogicalTypeUtils::getRowLayoutSize(structField->dataType);
    }
}

void StructVector::copyToRowData(const ValueVector* vector, uint32_t pos, uint8_t* rowData,
    InMemOverflowBuffer* rowOverflowBuffer) {
    // The storage structure of STRUCT type in factorizedTable is:
    // [NULLBYTES, FIELD1, FIELD2, ...]
    auto& structFields = StructVector::getFieldVectors(vector);
    NullBuffer::initNullBytes(rowData, structFields.size());
    auto structNullBytes = rowData;
    auto structValues = structNullBytes + NullBuffer::getNumBytesForNullValues(structFields.size());
    for (auto i = 0u; i < structFields.size(); i++) {
        auto structField = structFields[i];
        if (structField->isNull(pos)) {
            NullBuffer::setNull(structNullBytes, i);
        } else {
            structField->copyToRowData(pos, structValues, rowOverflowBuffer);
        }
        structValues += LogicalTypeUtils::getRowLayoutSize(structField->dataType);
    }
}

void StructVector::copyFromVectorData(ValueVector* dstVector, const uint8_t* dstData,
    const ValueVector* srcVector, const uint8_t* srcData) {
    auto& srcPos = (*(struct_entry_t*)srcData).pos;
    auto& dstPos = (*(struct_entry_t*)dstData).pos;
    auto& srcFieldVectors = getFieldVectors(srcVector);
    auto& dstFieldVectors = getFieldVectors(dstVector);
    for (auto i = 0u; i < srcFieldVectors.size(); i++) {
        auto srcFieldVector = srcFieldVectors[i];
        auto dstFieldVector = dstFieldVectors[i];
        if (srcFieldVector->isNull(srcPos)) {
            dstFieldVector->setNull(dstPos, true /* isNull */);
        } else {
            auto srcFieldVectorData =
                srcFieldVector->getData() + srcFieldVector->getNumBytesPerValue() * srcPos;
            auto dstFieldVectorData =
                dstFieldVector->getData() + dstFieldVector->getNumBytesPerValue() * dstPos;
            dstFieldVector->copyFromVectorData(
                dstFieldVectorData, srcFieldVector.get(), srcFieldVectorData);
        }
    }
}

} // namespace common
} // namespace kuzu
