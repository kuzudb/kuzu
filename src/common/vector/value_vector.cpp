#include "common/vector/value_vector.h"

#include "common/in_mem_overflow_buffer_utils.h"
#include "common/vector/auxiliary_buffer.h"
#include "common/vector/value_vector_utils.h"

namespace kuzu {
namespace common {

ValueVector::ValueVector(DataType dataType, storage::MemoryManager* memoryManager)
    : dataType{std::move(dataType)} {
    // TODO(Ziyi): remove this if/else statement once we removed the ku_list.
    numBytesPerValue = this->dataType.typeID == VAR_LIST ? sizeof(common::list_entry_t) :
                                                           Types::getDataTypeSize(this->dataType);
    valueBuffer = std::make_unique<uint8_t[]>(numBytesPerValue * DEFAULT_VECTOR_CAPACITY);
    nullMask = std::make_unique<NullMask>();
    auxiliaryBuffer = AuxiliaryBufferFactory::getAuxiliaryBuffer(this->dataType, memoryManager);
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
void ValueVector::setValue(uint32_t pos, common::list_entry_t val) {
    ((list_entry_t*)valueBuffer.get())[pos] = val;
}

template<>
void ValueVector::setValue(uint32_t pos, std::string val) {
    StringVector::addString(this, pos, val.data(), val.length());
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
