#pragma once

#include "common/assert.h"
#include "common/string_utils.h"
#include "storage/store/column_chunk.h"

namespace kuzu {
namespace storage {

class StringColumnChunk : public ColumnChunk {
    using string_offset_t = uint64_t;
    using string_index_t = uint32_t;

public:
    explicit StringColumnChunk(
        std::unique_ptr<common::LogicalType> dataType, uint64_t capacity, bool enableCompression);

    void resetToEmpty() final;
    void append(common::ValueVector* vector) final;
    void append(ColumnChunk* other, common::offset_t startPosInOtherChunk,
        uint32_t numValuesToAppend) final;

    void write(common::ValueVector* vector, common::offset_t offsetInVector,
        common::offset_t offsetInChunk) final;
    void write(common::ValueVector* valueVector, common::ValueVector* offsetInChunkVector,
        bool isCSR) final;
    void write(ColumnChunk* srcChunk, common::offset_t srcOffsetInChunk,
        common::offset_t dstOffsetInChunk, common::offset_t numValuesToCopy) override;
    void copy(ColumnChunk* srcChunk, common::offset_t srcOffsetInChunk,
        common::offset_t dstOffsetInChunk, common::offset_t numValuesToCopy) final;

    template<typename T>
    T getValue(common::offset_t /*pos*/) const {
        KU_UNREACHABLE;
    }

    uint64_t getStringLength(common::offset_t pos) const {
        auto index = ColumnChunk::getValue<string_index_t>(pos);
        if (index + 1 < offsetChunk->getNumValues()) {
            KU_ASSERT(offsetChunk->getValue<string_offset_t>(index + 1) >=
                      offsetChunk->getValue<string_offset_t>(index));
            return offsetChunk->getValue<string_offset_t>(index + 1) -
                   offsetChunk->getValue<string_offset_t>(index);
        }
        return stringDataChunk->getNumValues() - offsetChunk->getValue<string_offset_t>(index);
    }

    ColumnChunk* getDataChunk() { return stringDataChunk.get(); }
    ColumnChunk* getOffsetChunk() { return offsetChunk.get(); }

    void finalize() final;

private:
    void appendStringColumnChunk(StringColumnChunk* other, common::offset_t startPosInOtherChunk,
        uint32_t numValuesToAppend);

    void setValueFromString(const char* value, uint64_t length, uint64_t pos);

private:
    // String data is stored as a UINT8 chunk, using the numValues in the chunk to track the number
    // of characters stored.
    std::unique_ptr<ColumnChunk> stringDataChunk;
    std::unique_ptr<ColumnChunk> offsetChunk;
    std::unordered_map<std::string, string_index_t, common::StringUtils::string_hash,
        std::equal_to<>>
        indexTable;
    // If we never update a value, we don't need to prune unused strings in finalize
    bool needFinalize;
};

// STRING
template<>
std::string StringColumnChunk::getValue<std::string>(common::offset_t pos) const;
template<>
std::string_view StringColumnChunk::getValue<std::string_view>(common::offset_t pos) const;

} // namespace storage
} // namespace kuzu
