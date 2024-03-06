#pragma once

#include "storage/store/column_chunk.h"

namespace kuzu {
namespace storage {

class DictionaryChunk {
public:
    using string_offset_t = uint64_t;
    using string_index_t = uint32_t;

    DictionaryChunk(uint64_t capacity, bool enableCompression);
    // A pointer to the dictionary chunk is stored in the StringOps for the indexTable
    // and can't be modified easily. Moving would invalidate that pointer
    DictionaryChunk(DictionaryChunk&& other) = delete;

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

    struct DictionaryEntry {
        string_index_t index;

        std::string_view get(const DictionaryChunk& dict) const { return dict.getString(index); }
    };

    struct StringOps {
        explicit StringOps(const DictionaryChunk* dict) : dict(dict) {}
        const DictionaryChunk* dict;
        using hash_type = std::hash<std::string_view>;
        using is_transparent = void;

        std::size_t operator()(const DictionaryEntry& entry) const {
            return std::hash<std::string_view>()(entry.get(*dict));
        }
        std::size_t operator()(const char* str) const { return hash_type{}(str); }
        std::size_t operator()(std::string_view str) const { return hash_type{}(str); }
        std::size_t operator()(std::string const& str) const { return hash_type{}(str); }

        bool operator()(const DictionaryEntry& lhs, const DictionaryEntry& rhs) const {
            return lhs.get(*dict) == rhs.get(*dict);
        }
        bool operator()(const DictionaryEntry& lhs, std::string_view rhs) const {
            return lhs.get(*dict) == rhs;
        }
        bool operator()(std::string_view lhs, const DictionaryEntry& rhs) const {
            return lhs == rhs.get(*dict);
        }
    };

    std::unordered_set<DictionaryEntry, StringOps /*hash*/, StringOps /*equals*/> indexTable;
};
} // namespace storage
} // namespace kuzu
