#pragma once

#include <string>

namespace graphflow {
namespace storage {

using namespace std;

class ListsMetadataConstants {
public:
    // All lists (pageList, defined later) are broken in pieces (called a pageListGroups) of size
    // PAGE_LIST_GROUP_SIZE + 1 each and chained together. In each piece, there are
    // PAGE_LIST_GROUP_SIZE elements of that list and the offset to the next pageListGroups in the
    // blob that contains all pageListGroups of all lists.
    static constexpr uint32_t PAGE_LIST_GROUP_SIZE = 3;

    // Metadata file suffixes
    inline static const string CHUNK_PAGE_LIST_HEAD_IDX_MAP_SUFFIX = ".chunks.f1";
    inline static const string CHUNK_PAGE_LISTS_SUFFIX = ".chunks.f2";
    inline static const string LARGE_LISTS_PAGE_LIST_HEAD_IDX_MAP_SUFFIX = ".largeLists.f1";
    inline static const string LARGE_LISTS_PAGE_LISTS_SUFFIX = ".largeLists.f2";
};

} // namespace storage
} // namespace graphflow
