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

} // namespace common
} // namespace kuzu
