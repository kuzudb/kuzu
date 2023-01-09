#include "common/vector/value_vector.h"

#include "common/in_mem_overflow_buffer_utils.h"

namespace kuzu {
namespace common {

ValueVector::ValueVector(DataType dataType, MemoryManager* memoryManager)
    : dataType{std::move(dataType)} {
    valueBuffer =
        make_unique<uint8_t[]>(Types::getDataTypeSize(this->dataType) * DEFAULT_VECTOR_CAPACITY);
    if (needOverflowBuffer()) {
        assert(memoryManager != nullptr);
        inMemOverflowBuffer = make_unique<InMemOverflowBuffer>(memoryManager);
    }
    nullMask = make_unique<NullMask>();
    numBytesPerValue = Types::getDataTypeSize(this->dataType);
}

void ValueVector::addString(uint32_t pos, char* value, uint64_t len) const {
    assert(dataType.typeID == STRING);
    auto& entry = ((ku_string_t*)getData())[pos];
    InMemOverflowBufferUtils::copyString(value, len, entry, *inMemOverflowBuffer);
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

template<>
void ValueVector::setValue(uint32_t pos, string val) {
    addString(pos, val.data(), val.length());
}

template void ValueVector::setValue<nodeID_t>(uint32_t pos, nodeID_t val);
template void ValueVector::setValue<bool>(uint32_t pos, bool val);
template void ValueVector::setValue<int64_t>(uint32_t pos, int64_t val);
template void ValueVector::setValue<hash_t>(uint32_t pos, hash_t val);
template void ValueVector::setValue<double_t>(uint32_t pos, double_t val);
template void ValueVector::setValue<date_t>(uint32_t pos, date_t val);
template void ValueVector::setValue<timestamp_t>(uint32_t pos, timestamp_t val);
template void ValueVector::setValue<interval_t>(uint32_t pos, interval_t val);
template void ValueVector::setValue<ku_string_t>(uint32_t pos, ku_string_t val);
template void ValueVector::setValue<ku_list_t>(uint32_t pos, ku_list_t val);

void ValueVector::setLiteral(uint32_t pos, const common::Literal& literal) {
    assert(dataType == literal.dataType);
    if (literal.isNull()) {
        setNull(pos, true);
        return;
    }
    auto size = Types::getDataTypeSize(dataType);
    copyLiteral(getData() + size * pos, literal);
}

void ValueVector::copyLiteral(uint8_t* dest, const common::Literal& literal) {
    auto size = Types::getDataTypeSize(literal.dataType);
    switch (literal.dataType.typeID) {
    case INT64: {
        memcpy(dest, &literal.val.int64Val, size);
    } break;
    case DOUBLE: {
        memcpy(dest, &literal.val.doubleVal, size);
    } break;
    case BOOL: {
        memcpy(dest, &literal.val.booleanVal, size);
    } break;
    case DATE: {
        memcpy(dest, &literal.val.dateVal, size);
    } break;
    case TIMESTAMP: {
        memcpy(dest, &literal.val.timestampVal, size);
    } break;
    case INTERVAL: {
        memcpy(dest, &literal.val.intervalVal, size);
    } break;
    case STRING: {
        InMemOverflowBufferUtils::copyString(literal.strVal.data(), literal.strVal.length(),
            *(ku_string_t*)dest, getOverflowBuffer());
    } break;
    case LIST: {
        auto& entry = *(ku_list_t*)dest;
        auto numElements = literal.listVal.size();
        auto elementSize = Types::getDataTypeSize(*dataType.childType);
        InMemOverflowBufferUtils::allocateSpaceForList(
            entry, numElements * elementSize, getOverflowBuffer());
        entry.size = numElements;
        for (auto i = 0u; i < numElements; ++i) {
            copyLiteral((uint8_t*)entry.overflowPtr + i * elementSize, literal.listVal[i]);
        }
    } break;
    default:
        throw NotImplementedException(
            "Unimplemented setLiteral() for type " + Types::dataTypeToString(dataType));
    }
}

} // namespace common
} // namespace kuzu
