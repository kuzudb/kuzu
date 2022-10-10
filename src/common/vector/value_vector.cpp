#include "src/common/include/vector/value_vector.h"

#include "src/common/include/overflow_buffer_utils.h"
#include "src/common/types/include/value.h"

namespace graphflow {
namespace common {

ValueVector::ValueVector(DataType dataType, MemoryManager* memoryManager)
    : dataType{move(dataType)} {
    valueBuffer =
        make_unique<uint8_t[]>(Types::getDataTypeSize(this->dataType) * DEFAULT_VECTOR_CAPACITY);
    values = valueBuffer.get();
    if (needOverflowBuffer()) {
        assert(memoryManager != nullptr);
        overflowBuffer = make_unique<OverflowBuffer>(memoryManager);
    }
    nullMask = make_unique<NullMask>();
    numBytesPerValue = Types::getDataTypeSize(this->dataType);
}

void ValueVector::addString(uint64_t pos, char* value, uint64_t len) const {
    assert(dataType.typeID == STRING);
    auto vectorData = (gf_string_t*)values;
    auto& result = vectorData[pos];
    OverflowBufferUtils::copyString(value, len, result, *overflowBuffer);
}

void ValueVector::addString(uint64_t pos, string value) const {
    addString(pos, value.data(), value.length());
}

bool NodeIDVector::discardNull(ValueVector& vector) {
    if (vector.state->isFlat()) {
        return !vector.isNull(vector.state->getPositionOfCurrIdx());
    } else {
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
}

} // namespace common
} // namespace graphflow
