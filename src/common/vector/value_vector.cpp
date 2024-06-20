#include "common/vector/value_vector.h"

#include "common/constants.h"
#include "common/exception/runtime.h"
#include "common/null_buffer.h"
#include "common/types/blob.h"
#include "common/types/value/nested.h"
#include "common/types/value/value.h"
#include "common/vector/auxiliary_buffer.h"

namespace kuzu {
namespace common {

ValueVector::ValueVector(LogicalType dataType, storage::MemoryManager* memoryManager)
    : dataType{std::move(dataType)}, nullMask{DEFAULT_VECTOR_CAPACITY} {
    if (this->dataType.getLogicalTypeID() == LogicalTypeID::ANY) {
        // LCOV_EXCL_START
        // Alternatively we can assign a default type here but I don't think it's a good practice.
        throw RuntimeException("Trying to a create a vector with ANY type. This should not happen. "
                               "Data type is expected to be resolved during binding.");
        // LCOV_EXCL_STOP
    }
    numBytesPerValue = getDataTypeSize(this->dataType);
    initializeValueBuffer();
    auxiliaryBuffer = AuxiliaryBufferFactory::getAuxiliaryBuffer(this->dataType, memoryManager);
}

void ValueVector::setState(const std::shared_ptr<DataChunkState>& state_) {
    this->state = state_;
    if (dataType.getPhysicalType() == PhysicalTypeID::STRUCT) {
        auto childrenVectors = StructVector::getFieldVectors(this);
        for (auto& childVector : childrenVectors) {
            childVector->setState(state_);
        }
    }
}

bool ValueVector::discardNull(ValueVector& vector) {
    if (vector.hasNoNullsGuarantee()) {
        return true;
    } else {
        auto selectedPos = 0u;
        if (vector.state->getSelVector().isUnfiltered()) {
            auto buffer = vector.state->getSelVectorUnsafe().getMultableBuffer();
            for (auto i = 0u; i < vector.state->getSelVector().getSelSize(); i++) {
                buffer[selectedPos] = i;
                selectedPos += !vector.isNull(i);
            }
            vector.state->getSelVectorUnsafe().setToFiltered();
        } else {
            for (auto i = 0u; i < vector.state->getSelVector().getSelSize(); i++) {
                auto pos = vector.state->getSelVector()[i];
                vector.state->getSelVectorUnsafe()[i] = pos;
                selectedPos += !vector.isNull(pos);
            }
        }
        vector.state->getSelVectorUnsafe().setSelSize(selectedPos);
        return selectedPos > 0;
    }
}

bool ValueVector::setNullFromBits(const uint64_t* srcNullEntries, uint64_t srcOffset,
    uint64_t dstOffset, uint64_t numBitsToCopy, bool invert) {
    return nullMask.copyFromNullBits(srcNullEntries, srcOffset, dstOffset, numBitsToCopy, invert);
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
    case PhysicalTypeID::ARRAY:
    case PhysicalTypeID::LIST: {
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

void ValueVector::copyToRowData(uint32_t pos, uint8_t* rowData,
    InMemOverflowBuffer* rowOverflowBuffer) const {
    switch (dataType.getPhysicalType()) {
    case PhysicalTypeID::STRUCT: {
        StructVector::copyToRowData(this, pos, rowData, rowOverflowBuffer);
    } break;
    case PhysicalTypeID::ARRAY:
    case PhysicalTypeID::LIST: {
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

void ValueVector::copyFromVectorData(uint8_t* dstData, const ValueVector* srcVector,
    const uint8_t* srcVectorData) {
    KU_ASSERT(srcVector->dataType.getPhysicalType() == dataType.getPhysicalType());
    switch (srcVector->dataType.getPhysicalType()) {
    case PhysicalTypeID::STRUCT: {
        StructVector::copyFromVectorData(this, dstData, srcVector, srcVectorData);
    } break;
    case PhysicalTypeID::ARRAY:
    case PhysicalTypeID::LIST: {
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

void ValueVector::copyFromVectorData(uint64_t dstPos, const ValueVector* srcVector,
    uint64_t srcPos) {
    setNull(dstPos, srcVector->isNull(srcPos));
    if (!isNull(dstPos)) {
        copyFromVectorData(getData() + dstPos * getNumBytesPerValue(), srcVector,
            srcVector->getData() + srcPos * srcVector->getNumBytesPerValue());
    }
}

void ValueVector::copyFromValue(uint64_t pos, const Value& value) {
    if (value.isNull()) {
        setNull(pos, true);
        return;
    }
    setNull(pos, false);
    auto dstValue = valueBuffer.get() + pos * numBytesPerValue;
    switch (dataType.getPhysicalType()) {
    case PhysicalTypeID::INT64: {
        memcpy(dstValue, &value.val.int64Val, numBytesPerValue);
    } break;
    case PhysicalTypeID::INT32: {
        memcpy(dstValue, &value.val.int32Val, numBytesPerValue);
    } break;
    case PhysicalTypeID::INT16: {
        memcpy(dstValue, &value.val.int16Val, numBytesPerValue);
    } break;
    case PhysicalTypeID::INT8: {
        memcpy(dstValue, &value.val.int8Val, numBytesPerValue);
    } break;
    case PhysicalTypeID::UINT64: {
        memcpy(dstValue, &value.val.uint64Val, numBytesPerValue);
    } break;
    case PhysicalTypeID::UINT32: {
        memcpy(dstValue, &value.val.uint32Val, numBytesPerValue);
    } break;
    case PhysicalTypeID::UINT16: {
        memcpy(dstValue, &value.val.uint16Val, numBytesPerValue);
    } break;
    case PhysicalTypeID::UINT8: {
        memcpy(dstValue, &value.val.uint8Val, numBytesPerValue);
    } break;
    case PhysicalTypeID::INT128: {
        memcpy(dstValue, &value.val.int128Val, numBytesPerValue);
    } break;
    case PhysicalTypeID::DOUBLE: {
        memcpy(dstValue, &value.val.doubleVal, numBytesPerValue);
    } break;
    case PhysicalTypeID::FLOAT: {
        memcpy(dstValue, &value.val.floatVal, numBytesPerValue);
    } break;
    case PhysicalTypeID::BOOL: {
        memcpy(dstValue, &value.val.booleanVal, numBytesPerValue);
    } break;
    case PhysicalTypeID::INTERVAL: {
        memcpy(dstValue, &value.val.intervalVal, numBytesPerValue);
    } break;
    case PhysicalTypeID::STRING: {
        StringVector::addString(this, *(ku_string_t*)dstValue, value.strVal.data(),
            value.strVal.length());
    } break;
    case PhysicalTypeID::ARRAY:
    case PhysicalTypeID::LIST: {
        auto listEntry = reinterpret_cast<list_entry_t*>(dstValue);
        auto numValues = NestedVal::getChildrenSize(&value);
        *listEntry = ListVector::addList(this, numValues);
        auto dstDataVector = ListVector::getDataVector(this);
        for (auto i = 0u; i < numValues; ++i) {
            auto childVal = NestedVal::getChildVal(&value, i);
            dstDataVector->setNull(listEntry->offset + i, childVal->isNull());
            if (!childVal->isNull()) {
                dstDataVector->copyFromValue(listEntry->offset + i,
                    *NestedVal::getChildVal(&value, i));
            }
        }
    } break;
    case PhysicalTypeID::STRUCT: {
        auto structFields = StructVector::getFieldVectors(this);
        for (auto i = 0u; i < structFields.size(); ++i) {
            structFields[i]->copyFromValue(pos, *NestedVal::getChildVal(&value, i));
        }
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
}

std::unique_ptr<Value> ValueVector::getAsValue(uint64_t pos) const {
    if (isNull(pos)) {
        return Value::createNullValue(dataType).copy();
    }
    auto value = Value::createDefaultValue(dataType).copy();
    switch (dataType.getPhysicalType()) {
    case PhysicalTypeID::INT64: {
        value->val.int64Val = getValue<int64_t>(pos);
    } break;
    case PhysicalTypeID::INT32: {
        value->val.int32Val = getValue<int32_t>(pos);
    } break;
    case PhysicalTypeID::INT16: {
        value->val.int16Val = getValue<int16_t>(pos);
    } break;
    case PhysicalTypeID::INT8: {
        value->val.int8Val = getValue<int8_t>(pos);
    } break;
    case PhysicalTypeID::UINT64: {
        value->val.uint64Val = getValue<uint64_t>(pos);
    } break;
    case PhysicalTypeID::UINT32: {
        value->val.uint32Val = getValue<uint32_t>(pos);
    } break;
    case PhysicalTypeID::UINT16: {
        value->val.uint16Val = getValue<uint16_t>(pos);
    } break;
    case PhysicalTypeID::UINT8: {
        value->val.uint8Val = getValue<uint8_t>(pos);
    } break;
    case PhysicalTypeID::INT128: {
        value->val.int128Val = getValue<int128_t>(pos);
    } break;
    case PhysicalTypeID::DOUBLE: {
        value->val.doubleVal = getValue<double>(pos);
    } break;
    case PhysicalTypeID::FLOAT: {
        value->val.floatVal = getValue<float>(pos);
    } break;
    case PhysicalTypeID::BOOL: {
        value->val.booleanVal = getValue<bool>(pos);
    } break;
    case PhysicalTypeID::INTERVAL: {
        value->val.intervalVal = getValue<interval_t>(pos);
    } break;
    case PhysicalTypeID::STRING: {
        value->strVal = getValue<ku_string_t>(pos).getAsString();
    } break;
    case PhysicalTypeID::ARRAY:
    case PhysicalTypeID::LIST: {
        auto dataVector = ListVector::getDataVector(this);
        auto listEntry = getValue<list_entry_t>(pos);
        std::vector<std::unique_ptr<Value>> children;
        children.reserve(listEntry.size);
        for (auto i = 0u; i < listEntry.size; ++i) {
            children.push_back(dataVector->getAsValue(listEntry.offset + i));
        }
        value->childrenSize = children.size();
        value->children = std::move(children);
    } break;
    case PhysicalTypeID::STRUCT: {
        auto& fieldVectors = StructVector::getFieldVectors(this);
        std::vector<std::unique_ptr<Value>> children;
        children.reserve(fieldVectors.size());
        for (auto& fieldVector : fieldVectors) {
            children.push_back(fieldVector->getAsValue(pos));
        }
        value->childrenSize = children.size();
        value->children = std::move(children);
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
    return value;
}

void ValueVector::resetAuxiliaryBuffer() {
    switch (dataType.getPhysicalType()) {
    case PhysicalTypeID::STRING: {
        ku_dynamic_cast<AuxiliaryBuffer*, StringAuxiliaryBuffer*>(auxiliaryBuffer.get())
            ->resetOverflowBuffer();
        return;
    }
    case PhysicalTypeID::ARRAY:
    case PhysicalTypeID::LIST: {
        auto listAuxiliaryBuffer =
            ku_dynamic_cast<AuxiliaryBuffer*, ListAuxiliaryBuffer*>(auxiliaryBuffer.get());
        listAuxiliaryBuffer->resetSize();
        listAuxiliaryBuffer->getDataVector()->resetAuxiliaryBuffer();
        return;
    }
    case PhysicalTypeID::STRUCT: {
        auto structAuxiliaryBuffer =
            ku_dynamic_cast<AuxiliaryBuffer*, StructAuxiliaryBuffer*>(auxiliaryBuffer.get());
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
    case PhysicalTypeID::STRUCT: {
        return sizeof(struct_entry_t);
    }
    case PhysicalTypeID::ARRAY:
    case PhysicalTypeID::LIST: {
        return sizeof(list_entry_t);
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

template KUZU_API void ValueVector::setValue<nodeID_t>(uint32_t pos, nodeID_t val);
template KUZU_API void ValueVector::setValue<bool>(uint32_t pos, bool val);
template KUZU_API void ValueVector::setValue<int64_t>(uint32_t pos, int64_t val);
template KUZU_API void ValueVector::setValue<int32_t>(uint32_t pos, int32_t val);
template KUZU_API void ValueVector::setValue<int16_t>(uint32_t pos, int16_t val);
template KUZU_API void ValueVector::setValue<int8_t>(uint32_t pos, int8_t val);
template KUZU_API void ValueVector::setValue<uint64_t>(uint32_t pos, uint64_t val);
template KUZU_API void ValueVector::setValue<uint32_t>(uint32_t pos, uint32_t val);
template KUZU_API void ValueVector::setValue<uint16_t>(uint32_t pos, uint16_t val);
template KUZU_API void ValueVector::setValue<uint8_t>(uint32_t pos, uint8_t val);
template KUZU_API void ValueVector::setValue<int128_t>(uint32_t pos, int128_t val);
template KUZU_API void ValueVector::setValue<double>(uint32_t pos, double val);
template KUZU_API void ValueVector::setValue<float>(uint32_t pos, float val);
template KUZU_API void ValueVector::setValue<date_t>(uint32_t pos, date_t val);
template KUZU_API void ValueVector::setValue<timestamp_t>(uint32_t pos, timestamp_t val);
template KUZU_API void ValueVector::setValue<timestamp_ns_t>(uint32_t pos, timestamp_ns_t val);
template KUZU_API void ValueVector::setValue<timestamp_ms_t>(uint32_t pos, timestamp_ms_t val);
template KUZU_API void ValueVector::setValue<timestamp_sec_t>(uint32_t pos, timestamp_sec_t val);
template KUZU_API void ValueVector::setValue<timestamp_tz_t>(uint32_t pos, timestamp_tz_t val);
template KUZU_API void ValueVector::setValue<interval_t>(uint32_t pos, interval_t val);
template KUZU_API void ValueVector::setValue<list_entry_t>(uint32_t pos, list_entry_t val);

template<>
void ValueVector::setValue(uint32_t pos, ku_string_t val) {
    StringVector::addString(this, pos, val);
}
template<>
void ValueVector::setValue(uint32_t pos, std::string val) {
    StringVector::addString(this, pos, val.data(), val.length());
}
template<>
void ValueVector::setValue(uint32_t pos, std::string_view val) {
    StringVector::addString(this, pos, val.data(), val.length());
}

void ValueVector::setNull(uint32_t pos, bool isNull) {
    nullMask.setNull(pos, isNull);
}

void StringVector::addString(ValueVector* vector, uint32_t vectorPos, ku_string_t& srcStr) {
    KU_ASSERT(vector->dataType.getPhysicalType() == PhysicalTypeID::STRING);
    auto stringBuffer =
        ku_dynamic_cast<AuxiliaryBuffer*, StringAuxiliaryBuffer*>(vector->auxiliaryBuffer.get());
    auto& dstStr = vector->getValue<ku_string_t>(vectorPos);
    if (ku_string_t::isShortString(srcStr.len)) {
        dstStr.setShortString(srcStr);
    } else {
        dstStr.overflowPtr = reinterpret_cast<uint64_t>(stringBuffer->allocateOverflow(srcStr.len));
        dstStr.setLongString(srcStr);
    }
}

void StringVector::addString(ValueVector* vector, uint32_t vectorPos, const char* srcStr,
    uint64_t length) {
    KU_ASSERT(vector->dataType.getPhysicalType() == PhysicalTypeID::STRING);
    auto stringBuffer =
        ku_dynamic_cast<AuxiliaryBuffer*, StringAuxiliaryBuffer*>(vector->auxiliaryBuffer.get());
    auto& dstStr = vector->getValue<ku_string_t>(vectorPos);
    if (ku_string_t::isShortString(length)) {
        dstStr.setShortString(srcStr, length);
    } else {
        dstStr.overflowPtr = reinterpret_cast<uint64_t>(stringBuffer->allocateOverflow(length));
        dstStr.setLongString(srcStr, length);
    }
}

void StringVector::addString(ValueVector* vector, uint32_t vectorPos, const std::string& srcStr) {
    addString(vector, vectorPos, srcStr.data(), srcStr.length());
}

ku_string_t& StringVector::reserveString(ValueVector* vector, uint32_t vectorPos, uint64_t length) {
    KU_ASSERT(vector->dataType.getPhysicalType() == PhysicalTypeID::STRING);
    auto stringBuffer =
        ku_dynamic_cast<AuxiliaryBuffer*, StringAuxiliaryBuffer*>(vector->auxiliaryBuffer.get());
    auto& dstStr = vector->getValue<ku_string_t>(vectorPos);
    dstStr.len = length;
    if (!ku_string_t::isShortString(length)) {
        dstStr.overflowPtr = reinterpret_cast<uint64_t>(stringBuffer->allocateOverflow(length));
    }
    return dstStr;
}

void StringVector::reserveString(ValueVector* vector, ku_string_t& dstStr, uint64_t length) {
    KU_ASSERT(vector->dataType.getPhysicalType() == PhysicalTypeID::STRING);
    auto stringBuffer =
        ku_dynamic_cast<AuxiliaryBuffer*, StringAuxiliaryBuffer*>(vector->auxiliaryBuffer.get());
    dstStr.len = length;
    if (!ku_string_t::isShortString(length)) {
        dstStr.overflowPtr = reinterpret_cast<uint64_t>(stringBuffer->allocateOverflow(length));
    }
}

void StringVector::addString(ValueVector* vector, ku_string_t& dstStr, ku_string_t& srcStr) {
    KU_ASSERT(vector->dataType.getPhysicalType() == PhysicalTypeID::STRING);
    auto stringBuffer =
        ku_dynamic_cast<AuxiliaryBuffer*, StringAuxiliaryBuffer*>(vector->auxiliaryBuffer.get());
    if (ku_string_t::isShortString(srcStr.len)) {
        dstStr.setShortString(srcStr);
    } else {
        dstStr.overflowPtr = reinterpret_cast<uint64_t>(stringBuffer->allocateOverflow(srcStr.len));
        dstStr.setLongString(srcStr);
    }
}

void StringVector::addString(ValueVector* vector, ku_string_t& dstStr, const char* srcStr,
    uint64_t length) {
    KU_ASSERT(vector->dataType.getPhysicalType() == PhysicalTypeID::STRING);
    auto stringBuffer =
        ku_dynamic_cast<AuxiliaryBuffer*, StringAuxiliaryBuffer*>(vector->auxiliaryBuffer.get());
    if (ku_string_t::isShortString(length)) {
        dstStr.setShortString(srcStr, length);
    } else {
        dstStr.overflowPtr = reinterpret_cast<uint64_t>(stringBuffer->allocateOverflow(length));
        dstStr.setLongString(srcStr, length);
    }
}

void StringVector::addString(kuzu::common::ValueVector* vector, ku_string_t& dstStr,
    const std::string& srcStr) {
    addString(vector, dstStr, srcStr.data(), srcStr.length());
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
    KU_ASSERT(vector->dataType.getPhysicalType() == PhysicalTypeID::LIST ||
              vector->dataType.getPhysicalType() == PhysicalTypeID::ARRAY);
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
            resultDataVector->setNull(dstListValuePos, false);
            resultDataVector->copyFromRowData(dstListValuePos, srcListValues);
        }
        srcListValues += rowLayoutSize;
    }
}

void ListVector::copyToRowData(const ValueVector* vector, uint32_t pos, uint8_t* rowData,
    InMemOverflowBuffer* rowOverflowBuffer) {
    auto& srcListEntry = vector->getValue<list_entry_t>(pos);
    auto srcListDataVector = ListVector::getDataVector(vector);
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
            srcListDataVector->copyToRowData(srcListEntry.offset + i, dstListValues,
                rowOverflowBuffer);
        }
        dstListValues += dataRowLayoutSize;
    }
}

void ListVector::copyFromVectorData(ValueVector* dstVector, uint8_t* dstData,
    const ValueVector* srcVector, const uint8_t* srcData) {
    auto& srcListEntry = *(list_entry_t*)(srcData);
    auto& dstListEntry = *(list_entry_t*)(dstData);
    dstListEntry = addList(dstVector, srcListEntry.size);
    auto srcDataVector = getDataVector(srcVector);
    auto srcPos = srcListEntry.offset;
    auto dstDataVector = getDataVector(dstVector);
    auto dstPos = dstListEntry.offset;
    for (auto i = 0u; i < srcListEntry.size; i++) {
        dstDataVector->copyFromVectorData(dstPos++, srcDataVector, srcPos++);
    }
}

void ListVector::appendDataVector(kuzu::common::ValueVector* dstVector,
    kuzu::common::ValueVector* srcDataVector, uint64_t numValuesToAppend) {
    auto offset = getDataVectorSize(dstVector);
    resizeDataVector(dstVector, offset + numValuesToAppend);
    auto dstDataVector = getDataVector(dstVector);
    for (auto i = 0u; i < numValuesToAppend; i++) {
        dstDataVector->copyFromVectorData(offset + i, srcDataVector, i);
    }
}

void ListVector::sliceDataVector(ValueVector* vectorToSlice, uint64_t offset, uint64_t numValues) {
    if (offset == 0) {
        return;
    }
    for (auto i = 0u; i < numValues - offset; i++) {
        vectorToSlice->copyFromVectorData(i, vectorToSlice, i + offset);
    }
}

void StructVector::copyFromRowData(ValueVector* vector, uint32_t pos, const uint8_t* rowData) {
    KU_ASSERT(vector->dataType.getPhysicalType() == PhysicalTypeID::STRUCT);
    auto& structFields = getFieldVectors(vector);
    auto structNullBytes = rowData;
    auto structValues = structNullBytes + NullBuffer::getNumBytesForNullValues(structFields.size());
    for (auto i = 0u; i < structFields.size(); i++) {
        auto structField = structFields[i];
        if (NullBuffer::isNull(structNullBytes, i)) {
            structField->setNull(pos, true /* isNull */);
        } else {
            structField->setNull(pos, false /* isNull */);
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
        dstFieldVector->copyFromVectorData(dstPos, srcFieldVector.get(), srcPos);
    }
}

void RdfVariantVector::addString(ValueVector* vector, sel_t pos, ku_string_t str) {
    addString(vector, pos, (const char*)str.getData(), str.len);
}

void RdfVariantVector::addString(ValueVector* vector, common::sel_t pos, const char* str,
    uint32_t length) {
    auto typeVector = StructVector::getFieldVector(vector, 0).get();
    auto valVector = StructVector::getFieldVector(vector, 1).get();
    typeVector->setValue<uint8_t>(pos, static_cast<uint8_t>(LogicalTypeID::STRING));
    BlobVector::addBlob(valVector, pos, str, length);
}

template<typename T>
static void addRdfVariant(ValueVector* vector, sel_t pos, LogicalTypeID typeID, T val) {
    auto typeVector = StructVector::getFieldVector(vector, 0).get();
    auto valVector = StructVector::getFieldVector(vector, 1).get();
    typeVector->setValue<uint8_t>(pos, static_cast<uint8_t>(typeID));
    BlobVector::addBlob(valVector, pos, (uint8_t*)&val, sizeof(T));
}

template<>
void RdfVariantVector::add(ValueVector* vector, sel_t pos, int128_t val) {
    addRdfVariant(vector, pos, LogicalTypeID::INT128, val);
}
template<>
void RdfVariantVector::add(ValueVector* vector, sel_t pos, int64_t val) {
    addRdfVariant(vector, pos, LogicalTypeID::INT64, val);
}
template<>
void RdfVariantVector::add(ValueVector* vector, sel_t pos, int32_t val) {
    addRdfVariant(vector, pos, LogicalTypeID::INT32, val);
}
template<>
void RdfVariantVector::add(ValueVector* vector, sel_t pos, int16_t val) {
    addRdfVariant(vector, pos, LogicalTypeID::INT16, val);
}
template<>
void RdfVariantVector::add(ValueVector* vector, sel_t pos, int8_t val) {
    addRdfVariant(vector, pos, LogicalTypeID::INT8, val);
}
template<>
void RdfVariantVector::add(ValueVector* vector, sel_t pos, uint64_t val) {
    addRdfVariant(vector, pos, LogicalTypeID::UINT64, val);
}
template<>
void RdfVariantVector::add(ValueVector* vector, sel_t pos, uint32_t val) {
    addRdfVariant(vector, pos, LogicalTypeID::UINT32, val);
}
template<>
void RdfVariantVector::add(ValueVector* vector, sel_t pos, uint16_t val) {
    addRdfVariant(vector, pos, LogicalTypeID::UINT16, val);
}
template<>
void RdfVariantVector::add(ValueVector* vector, sel_t pos, uint8_t val) {
    addRdfVariant(vector, pos, LogicalTypeID::UINT8, val);
}
template<>
void RdfVariantVector::add(ValueVector* vector, sel_t pos, double val) {
    addRdfVariant(vector, pos, LogicalTypeID::DOUBLE, val);
}
template<>
void RdfVariantVector::add(ValueVector* vector, sel_t pos, float val) {
    addRdfVariant(vector, pos, LogicalTypeID::FLOAT, val);
}
template<>
void RdfVariantVector::add(ValueVector* vector, sel_t pos, blob_t val) {
    addRdfVariant(vector, pos, LogicalTypeID::BLOB, val);
}
template<>
void RdfVariantVector::add(ValueVector* vector, sel_t pos, bool val) {
    addRdfVariant(vector, pos, LogicalTypeID::BOOL, val);
}
template<>
void RdfVariantVector::add(ValueVector* vector, sel_t pos, date_t val) {
    addRdfVariant(vector, pos, LogicalTypeID::DATE, val);
}
template<>
void RdfVariantVector::add(ValueVector* vector, sel_t pos, timestamp_t val) {
    addRdfVariant(vector, pos, LogicalTypeID::TIMESTAMP, val);
}
template<>
void RdfVariantVector::add(ValueVector* vector, sel_t pos, interval_t val) {
    addRdfVariant(vector, pos, LogicalTypeID::INTERVAL, val);
}

} // namespace common
} // namespace kuzu
