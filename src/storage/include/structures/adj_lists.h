#pragma once

#include <vector>

#include "bitsery/bitsery.h"

#include "src/common/include/types.h"
#include "src/storage/include/buffer_manager.h"
#include "src/storage/include/structures/lists.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace loader {

class AdjListsLoaderHelper;
class RelsLoader;

} // namespace loader
} // namespace graphflow

namespace graphflow {
namespace storage {

class AdjLists;

// AdjListHeaders holds the headers of all lists in a single AdjLists structue.
class AdjListHeaders {
    friend class bitsery::Access;
    friend class AdjLists;
    friend class graphflow::loader::AdjListsLoaderHelper;
    friend class graphflow::loader::RelsLoader;

public:
    AdjListHeaders() = default;

private:
    AdjListHeaders(string path) { readFromFile(path); };

    template<typename S>
    void serialize(S& s);

    void saveToFile(const string& fname);
    void readFromFile(const string& fname);

private:
    // A header of a list is a uint32_t value the describes the following information about the
    // list: 1) type: small or large, 2) location of list in pages.

    // If the list is small, the layout of header is:
    //      1. MSB (31st bit) is reset
    //      2. 30-22 bits denote the index in the chunkPagesMaps[chunkId] where disk page ID is
    //      located, where chunkId = idx of the list / LISTS_CHUNK_SIZE.
    //      3. 21-11 bits denote the offset in the disk page.
    //      4. 10-0  bits denote the number of elements in the list.

    // If list is a large one (during initial data ingest, the criterion for being large is
    // whether or not a list fits in a single page), the layout of header is:
    //      1. MSB (31st bit) is set
    //      2. 30-0  bits denote the idx in the largeListsPagesMaps where the length and the disk
    //      page IDs are located.
    vector<uint32_t> headers;
};

// AdjLists inherits from Lists
class AdjLists : public Lists {

public:
    AdjLists(string fname, uint32_t numBytesPerLabel, uint32_t numBytesPerOffset,
        BufferManager& bufferManager)
        : Lists{fname, bufferManager}, headers{fname}, numBytesPerLabel{numBytesPerLabel},
          numBytesPerOffset{numBytesPerOffset} {};

private:
    AdjListHeaders headers;
    uint32_t numBytesPerLabel;
    uint32_t numBytesPerOffset;
};

} // namespace storage
} // namespace graphflow
