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
    explicit DataChunkState(uint64_t capacity) : fStateType{FStateType::UNFLAT}, originalSize{0} {
        selVector = std::make_shared<SelectionVector>(capacity);
    }

    // returns a dataChunkState for vectors holding a single value.
    static std::shared_ptr<DataChunkState> getSingleValueDataChunkState();

    inline void initOriginalAndSelectedSize(uint64_t size) {
        originalSize = size;
        selVector->selectedSize = size;
    }
    inline void setOriginalSize(uint64_t size) { originalSize = size; }
    inline uint64_t getOriginalSize() { return originalSize; }
    inline bool isFlat() const { return fStateType == FStateType::FLAT; }
    inline void setToFlat() { fStateType = FStateType::FLAT; }
    inline void setToUnflat() { fStateType = FStateType::UNFLAT; }

    inline uint64_t getNumSelectedValues() const { return selVector->selectedSize; }

    void slice(offset_t offset);

public:
    std::shared_ptr<SelectionVector> selVector;

private:
    FStateType fStateType;
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
