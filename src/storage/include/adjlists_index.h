#pragma once

#include <cstdint>
#include <vector>

#include "bitsery/bitsery.h"

#include "src/common/include/types.h"
#include "src/storage/include/buffer_manager.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace common {

class RelsLoader;

} // namespace common
} // namespace graphflow

namespace graphflow {
namespace storage {

class AdjLists;

class AdjListsMetadata {
    friend class graphflow::common::RelsLoader;
    friend class bitsery::Access;
    friend class AdjLists;

public:
    AdjListsMetadata() = default;

private:
    AdjListsMetadata(string path) { readFromFile(path); };

    template<typename S>
    void serialize(S& s);

    void saveToFile(const string& fname);
    void readFromFile(const string& fname);

private:
    // A header of a node is a uint32_t value the describes the following information about the
    // adjList of thsynchronouslyat node: 1) type: small or large {lAdjList} and, 2) location of
    // adjList.

    // If adjList is small, the layout of header is:
    //      1. MSB (31st bit) is reset
    //      2. 30-22 bits denote the index in the chunkPagesMaps[chunkId] where disk page ID is
    //      located, where chunkId = nodeOffset / 512.
    //      3. 21-11 bits denote the offset in the disk page.
    //      4. 10-0  bits denote the number of elements in the adjlist.

    // If adjList is a large one (during initial data ingest, the criterion for being large is
    // whether or not the adjList fits in a single page), the layout of header is:
    //      1. MSB (31st bit) is set
    //      2. 30-0  bits denote the idx in the lAdjListsPagesMaps where disk page IDs are located.
    vector<uint32_t> headers;

    // Holds the list of alloted disk page IDs for each chunk (that is a collection of regular
    // adjlists for 512 node offsets). The outer vector holds one vector per chunk, which holds the
    // pageIdxs of the pages used to hold the small adjlists of ADJLISTS_CHUNK_SIZE number of
    // vertices in one chunk.
    vector<vector<uint64_t>> chunksPagesMap;

    // Holds the list of alloted disk page IDs for the corresponding large adjList.
    vector<vector<uint64_t>> lAdjListsPagesMap;

    // total number of pages required for this adjListsIndex.
    uint64_t numPages;
};

class AdjLists {

public:
    AdjLists(string fname, uint32_t numBytesPerLabel, uint32_t numBytesPerOffset,
        BufferManager& bufferManager)
        : metadata{fname}, fileHandle{fname, metadata.numPages}, numBytesPerLabel{numBytesPerLabel},
          numBytesPerOffset{numBytesPerOffset}, bufferManager{bufferManager} {};

    inline static string getAdjListsIndexFname(
        const string& directory, gfLabel_t relLabel, gfLabel_t nodeLabel, Direction direction) {
        return directory + "/e-" + to_string(relLabel) + "-" + to_string(nodeLabel) + "-" +
               to_string(direction) + ".adjl";
    }

public:
    static const uint16_t ADJLISTS_CHUNK_SIZE = 512;

private:
    AdjListsMetadata metadata;
    FileHandle fileHandle;
    uint32_t numBytesPerLabel;
    uint32_t numBytesPerOffset;
    BufferManager& bufferManager;
};

} // namespace storage
} // namespace graphflow
