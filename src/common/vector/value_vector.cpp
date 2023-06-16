#include "common/vector/value_vector.h"

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
        return LogicalTypeUtils::getFixedTypeSize(type.getPhysicalType());
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

} // namespace common
} // namespace kuzu
