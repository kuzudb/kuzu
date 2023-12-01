#include "processor/operator/filtering_operator.h"

#include <cstring>

using namespace kuzu::common;

namespace kuzu {
namespace processor {

void SelVectorOverWriter::restoreSelVector(std::shared_ptr<SelectionVector>& selVector) {
    if (prevSelVector != nullptr) {
        selVector = prevSelVector;
    }
}

void SelVectorOverWriter::saveSelVector(std::shared_ptr<SelectionVector>& selVector) {
    if (prevSelVector == nullptr) {
        prevSelVector = selVector;
    }
    resetToCurrentSelVector(selVector);
}

void SelVectorOverWriter::resetToCurrentSelVector(std::shared_ptr<SelectionVector>& selVector) {
    currentSelVector->selectedSize = selVector->selectedSize;
    if (selVector->isUnfiltered()) {
        currentSelVector->resetSelectorToUnselected();
    } else {
        memcpy(currentSelVector->getSelectedPositionsBuffer(), selVector->selectedPositions,
            selVector->selectedSize * sizeof(sel_t));
        currentSelVector->resetSelectorToValuePosBuffer();
    }
    selVector = currentSelVector;
}

} // namespace processor
} // namespace kuzu
