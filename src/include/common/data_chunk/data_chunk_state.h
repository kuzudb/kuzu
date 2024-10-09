#pragma once

#include "common/data_chunk/sel_vector.h"

namespace kuzu {
namespace common {

// F stands for Factorization
enum class FStateType : uint8_t {
    FLAT = 0,
    UNFLAT = 1,
};

class DataChunkState {
public:
    DataChunkState() : DataChunkState(DEFAULT_VECTOR_CAPACITY) {}
    explicit DataChunkState(sel_t capacity) : fStateType{FStateType::UNFLAT} {
        selVector = std::make_shared<SelectionVector>(capacity);
    }

    // returns a dataChunkState for vectors holding a single value.
    static std::shared_ptr<DataChunkState> getSingleValueDataChunkState();

    void initOriginalAndSelectedSize(uint64_t size) { selVector->setSelSize(size); }
    bool isFlat() const { return fStateType == FStateType::FLAT; }
    void setToFlat() { fStateType = FStateType::FLAT; }
    void setToUnflat() { fStateType = FStateType::UNFLAT; }

    const SelectionVector& getSelVector() const { return *selVector; }
    sel_t getSelSize() const { return selVector->getSelSize(); }
    SelectionVector& getSelVectorUnsafe() { return *selVector; }
    std::shared_ptr<SelectionVector> getSelVectorShared() { return selVector; }
    void setSelVector(std::shared_ptr<SelectionVector> selVector_) {
        this->selVector = std::move(selVector_);
    }

    void slice(offset_t offset);

private:
    std::shared_ptr<SelectionVector> selVector;
    // TODO: We should get rid of `fStateType` and merge DataChunkState with SelectionVector.
    FStateType fStateType;
};

} // namespace common
} // namespace kuzu
