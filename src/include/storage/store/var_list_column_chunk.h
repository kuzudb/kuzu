#pragma once

#include "common/exception/not_implemented.h"
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
        dataColumnChunk->append(dataVector, dataColumnChunk->getNumValues());
    }

    inline uint64_t getNumValues() const { return dataColumnChunk->getNumValues(); }
};

class VarListColumnChunk : public ColumnChunk {
public:
    VarListColumnChunk(common::LogicalType dataType, uint64_t capacity, bool enableCompression);

    inline ColumnChunk* getDataColumnChunk() const {
        return varListDataColumnChunk->dataColumnChunk.get();
    }

    void resetToEmpty() final;

    void append(common::ValueVector* vector, common::offset_t startPosInChunk) final;
    inline void write(common::ValueVector* /*valueVector*/,
        common::ValueVector* /*offsetInChunkVector*/) override {
        // LCOV_EXCL_START
        throw common::NotImplementedException("VarListColumnChunk::write");
        // LCOV_EXCL_STOP
    }

    inline VarListDataColumnChunk* getVarListDataColumnChunk() {
        return varListDataColumnChunk.get();
    }
    inline void resizeDataColumnChunk(uint64_t numBytesForBuffer) {
        // TODO(bmwinger): This won't work properly for booleans (will be one eighth as many values
        // as could fit)
        auto numValues =
            varListDataColumnChunk->dataColumnChunk->getNumBytesPerValue() == 0 ?
                0 :
                numBytesForBuffer / varListDataColumnChunk->dataColumnChunk->getNumBytesPerValue();
        varListDataColumnChunk->resizeBuffer(numValues);
    }

protected:
    void copyListValues(const common::list_entry_t& entry, common::ValueVector* dataVector);

private:
    void append(ColumnChunk* other, common::offset_t startPosInOtherChunk,
        common::offset_t startPosInChunk, uint32_t numValuesToAppend) final;

    inline uint64_t getListLen(common::offset_t offset) const {
        return getListOffset(offset + 1) - getListOffset(offset);
    }
    inline common::offset_t getListOffset(common::offset_t offset) const {
        return offset == 0 ? 0 : getValue<uint64_t>(offset - 1);
    }

protected:
    bool enableCompression;
    std::unique_ptr<VarListDataColumnChunk> varListDataColumnChunk;
};

class AuxVarListColumnChunk : public VarListColumnChunk {
public:
    AuxVarListColumnChunk(common::LogicalType dataType, uint64_t capacity, bool enableCompression)
        : VarListColumnChunk{dataType, capacity * 2 /* for sizeof(list_entry_t) */,
              enableCompression},
          lastDataOffset{0} {}

    void resize(uint64_t newCapacity) final;
    void write(common::ValueVector* valueVector, common::ValueVector* offsetInChunkVector) final;

    std::unique_ptr<ColumnChunk> finalize() final;

private:
    uint64_t lastDataOffset;
};

} // namespace storage
} // namespace kuzu
