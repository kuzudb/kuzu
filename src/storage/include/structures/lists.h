#pragma once

#include "bitsery/bitsery.h"

#include "src/storage/include/file_handle.h"

namespace graphflow {
namespace loader {

class AdjListsLoaderHelper;
class RelsLoader;

} // namespace loader
} // namespace graphflow

namespace graphflow {
namespace storage {

class Lists;

// Lists Metadata holds the information necessary to locate a list in the collection of disk pages
// thats organizes and stores those lists.
class ListsMetadata {
    friend class graphflow::loader::AdjListsLoaderHelper;
    friend class graphflow::loader::RelsLoader;
    friend class Lists;
    friend class bitsery::Access;

public:
    ListsMetadata() = default;

private:
    ListsMetadata(string path) { readFromFile(path); };

    template<typename S>
    void serialize(S& s);

    void saveToFile(const string& fname);
    void readFromFile(const string& fname);

private:
    // Holds the list of alloted disk page IDs for each chunk (that is a collection of regular
    // adjlists for 512 node offsets). The outer vector holds one vector per chunk, which holds the
    // pageIdxs of the pages used to hold the small adjlists of LISTS_CHUNK_SIZE number of
    // vertices in one chunk.
    vector<vector<uint64_t>> chunksPagesMap;

    // Holds the list of alloted disk page IDs for the corresponding large lists.
    vector<vector<uint64_t>> largeListsPagesMap;

    // total number of pages required for oraganizing and storing the lists.
    uint64_t numPages;
};

// Lists is the basic structure that holds a set of lists {of adjacent edges or rel properties}.
class Lists {

protected:
    Lists(string fname, BufferManager& bufferManager)
        : metadata{fname}, fileHandle{fname, metadata.numPages}, bufferManager{bufferManager} {};

public:
    static const uint16_t LISTS_CHUNK_SIZE = 512;

private:
    ListsMetadata metadata;
    FileHandle fileHandle;
    BufferManager& bufferManager;
};

} // namespace storage
} // namespace graphflow
