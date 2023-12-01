#pragma once

#include "storage/store/column_chunk.h"

namespace kuzu {
namespace storage {

// TODO(Guodong): Let's simplify the data structure here by getting rid of this class.
struct VarListDataColumnChunk {
    std::unique_ptr<ColumnChunk> dataColumnChunk;
    uint64_t capacity;

    explicit VarListDataColumnChunk(std::unique_ptr<ColumnChunk> dataChunk)
        : dataColumnChunk{std::move(dataChunk)}, capacity{this->dataColumnChunk->capacity} {}

    void reset() const;

    void resizeBuffer(uint64_t numValues);

    inline void append(common::ValueVector* dataVector) const {
        dataColumnChunk->append(dataVector);
    }

    inline uint64_t getNumValues() const { return dataColumnChunk->getNumValues(); }
};

class VarListColumnChunk : public ColumnChunk {

public:
    VarListColumnChunk(
        std::unique_ptr<common::LogicalType> dataType, uint64_t capacity, bool enableCompression);

    inline ColumnChunk* getDataColumnChunk() const {
        return varListDataColumnChunk->dataColumnChunk.get();
    }

    void resetToEmpty() final;

    void append(common::ValueVector* vector) final;
    // Note: `write` assumes that no `append` will be called afterward.
    void write(common::ValueVector* vector, common::offset_t offsetInVector,
        common::offset_t offsetInChunk) final;
    void write(common::ValueVector* valueVector, common::ValueVector* offsetInChunkVector,
        bool isCSR) final;

    inline void resizeDataColumnChunk(uint64_t numBytesForBuffer) {
        // TODO(bmwinger): This won't work properly for booleans (will be one eighth as many values
        // as could fit)
        auto numValues =
            varListDataColumnChunk->dataColumnChunk->getNumBytesPerValue() == 0 ?
                0 :
                numBytesForBuffer / varListDataColumnChunk->dataColumnChunk->getNumBytesPerValue();
        varListDataColumnChunk->resizeBuffer(numValues);
    }

    void finalize() final;

protected:
    void copyListValues(const common::list_entry_t& entry, common::ValueVector* dataVector);

private:
    void append(ColumnChunk* other, common::offset_t startPosInOtherChunk,
        uint32_t numValuesToAppend) final;

    inline void initializeIndices() {
        indicesColumnChunk = ColumnChunkFactory::createColumnChunk(
            common::LogicalType::INT64(), false /* enableCompression */);
        indicesColumnChunk->getNullChunk()->resetToAllNull();
        for (auto i = 0u; i < numValues; i++) {
            indicesColumnChunk->setValue<common::offset_t>(i, i);
            indicesColumnChunk->getNullChunk()->setNull(i, nullChunk->isNull(i));
        }
        indicesColumnChunk->setNumValues(numValues);
    }
    inline uint64_t getListLen(common::offset_t offset) const {
        return getListOffset(offset + 1) - getListOffset(offset);
    }
    inline common::offset_t getListOffset(common::offset_t offset) const {
        return offset == 0 ? 0 : getValue<uint64_t>(offset - 1);
    }

    void resetFromOtherChunk(VarListColumnChunk* other);
    void appendNullList();

protected:
    bool enableCompression;
    std::unique_ptr<VarListDataColumnChunk> varListDataColumnChunk;
    // The following is needed to write var list to random positions in the column chunk.
    // We first append var list to the end of the column chunk. Then use indicesColumnChunk to track
    // where each var list data is inside the column chunk.
    // `needFinalize` is set to true whenever `write` is called.
    // During `finalize`, the whole column chunk will be re-written according to indices.
    bool needFinalize;
    std::unique_ptr<ColumnChunk> indicesColumnChunk;
};

} // namespace storage
} // namespace kuzu
