#pragma once

#include "storage/store/column_chunk.h"

namespace kuzu {
namespace storage {

struct VarListDataColumnChunk {
    std::unique_ptr<ColumnChunk> dataColumnChunk;
    uint64_t capacity;

    explicit VarListDataColumnChunk(std::unique_ptr<ColumnChunk> dataChunk)
        : dataColumnChunk{std::move(dataChunk)}, capacity{
                                                     common::StorageConstants::NODE_GROUP_SIZE} {}

    void reset() const;

    void resizeBuffer(uint64_t numValues);

    inline void append(common::ValueVector* dataVector) const {
        dataColumnChunk->append(dataVector, dataColumnChunk->getNumValues());
    }

    inline uint64_t getNumValues() const { return dataColumnChunk->getNumValues(); }

    inline void increaseNumValues(uint64_t numValues) const {
        dataColumnChunk->numValues += numValues;
    }
};

class VarListColumnChunk : public ColumnChunk {
public:
    VarListColumnChunk(common::LogicalType dataType, uint64_t capacity, bool enableCompression);

    inline ColumnChunk* getDataColumnChunk() const {
        return varListDataColumnChunk.dataColumnChunk.get();
    }

    void resetToEmpty() final;

    void append(common::ValueVector* vector, common::offset_t startPosInChunk) final;

    void write(common::ValueVector* valueVector, common::ValueVector* offsetInChunkVector);

    inline void resizeDataColumnChunk(uint64_t numBytesForBuffer) {
        // TODO(bmwinger): This won't work properly for booleans (will be one eighth as many values
        // as could fit)
        auto numValues =
            varListDataColumnChunk.dataColumnChunk->getNumBytesPerValue() == 0 ?
                0 :
                numBytesForBuffer / varListDataColumnChunk.dataColumnChunk->getNumBytesPerValue();
        varListDataColumnChunk.resizeBuffer(numValues);
    }

private:
    void append(ColumnChunk* other, common::offset_t startPosInOtherChunk,
        common::offset_t startPosInChunk, uint32_t numValuesToAppend) final;

    void copyListValues(const common::list_entry_t& entry, common::ValueVector* dataVector);

    inline uint64_t getListLen(common::offset_t offset) const {
        return getListOffset(offset + 1) - getListOffset(offset);
    }

    inline common::offset_t getListOffset(common::offset_t offset) const {
        return offset == 0 ? 0 : getValue<uint64_t>(offset - 1);
    }

private:
    VarListDataColumnChunk varListDataColumnChunk;
};

} // namespace storage
} // namespace kuzu
