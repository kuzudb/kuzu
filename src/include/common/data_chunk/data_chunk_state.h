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
    explicit DataChunkState(sel_t capacity) : fStateType{FStateType::UNFLAT}, originalSize{0} {
        selVector = std::make_shared<SelectionVector>(capacity);
    }

    // returns a dataChunkState for vectors holding a single value.
    static std::shared_ptr<DataChunkState> getSingleValueDataChunkState();

    void initOriginalAndSelectedSize(uint64_t size) {
        originalSize = size;
        selVector->setSelSize(size);
    }
    void setOriginalSize(uint64_t size) { originalSize = size; }
    uint64_t getOriginalSize() const { return originalSize; }
    bool isFlat() const { return fStateType == FStateType::FLAT; }
    void setToFlat() { fStateType = FStateType::FLAT; }
    void setToUnflat() { fStateType = FStateType::UNFLAT; }

    const SelectionVector& getSelVector() const { return *selVector; }
    SelectionVector& getSelVectorUnsafe() { return *selVector; }
    std::shared_ptr<SelectionVector> getSelVectorShared() { return selVector; }
    void setSelVector(std::shared_ptr<SelectionVector> selVector_) {
        this->selVector = std::move(selVector_);
    }

    void slice(offset_t offset);

private:
    std::shared_ptr<SelectionVector> selVector;
    FStateType fStateType;
    // TODO: Get rid of this field.
    // We need to keep track of originalSize of DataChunks to perform consistent scans of vectors
    // or lists. This is because all the vectors in a data chunk has to be the same length as they
    // share the same selectedPositions array.Therefore, if there is a scan after a filter on the
    // data chunk, the selectedSize of selVector might decrease, so the scan cannot know how much it
    // has to scan to generate a vector that is consistent with the rest of the vectors in the
    // data chunk.
    uint64_t originalSize;
};

} // namespace common
} // namespace kuzu
