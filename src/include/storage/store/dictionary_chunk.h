#pragma once

#include "common/string_utils.h"
#include "storage/store/column_chunk.h"

namespace kuzu {
namespace storage {

class DictionaryChunk {
public:
    using string_offset_t = uint64_t;
    using string_index_t = uint32_t;

    DictionaryChunk(uint64_t capacity, bool enableCompression);

    void resetToEmpty();

    uint64_t getStringLength(string_index_t index) const;

    string_index_t appendString(std::string_view val);

    std::string_view getString(string_index_t index) const;

    inline ColumnChunk* getStringDataChunk() const { return stringDataChunk.get(); }
    inline ColumnChunk* getOffsetChunk() const { return offsetChunk.get(); }

    bool sanityCheck() const;

private:
    bool enableCompression;
    // String data is stored as a UINT8 chunk, using the numValues in the chunk to track the number
    // of characters stored.
    std::unique_ptr<ColumnChunk> stringDataChunk;
    std::unique_ptr<ColumnChunk> offsetChunk;
    std::unordered_map<std::string, string_index_t, common::StringUtils::string_hash,
        std::equal_to<>>
        indexTable;
};
} // namespace storage
} // namespace kuzu
