#pragma once

#include "src/common/include/vector/node_vector.h"
#include "src/storage/include/structures/common.h"
#include "src/storage/include/structures/lists_aux_structures.h"

namespace graphflow {
namespace storage {

// BaseLists is the basic structure that holds a set of lists {of adjacent edges or rel properties}.
class BaseLists : public BaseColumnOrList {

public:
    void readValues(const nodeID_t& nodeID, const shared_ptr<ValueVector>& valueVector,
        uint64_t& adjListLen, const unique_ptr<VectorFrameHandle>& handle);

protected:
    BaseLists(const string& fname, size_t elementSize, shared_ptr<AdjListHeaders> headers,
        BufferManager& bufferManager)
        : BaseColumnOrList{fname, elementSize, bufferManager}, metadata{fname}, headers{headers} {};

public:
    constexpr static uint16_t LISTS_CHUNK_SIZE = 512;

protected:
    ListsMetadata metadata;
    shared_ptr<AdjListHeaders> headers;
};

template<typename T>
class Lists : public BaseLists {

public:
    Lists(const string& fname, shared_ptr<AdjListHeaders> headers, BufferManager& bufferManager)
        : BaseLists{fname, sizeof(T), headers, bufferManager} {};
};

template<>
class Lists<nodeID_t> : public BaseLists {

public:
    Lists(const string& fname, BufferManager& bufferManager,
        NodeIDCompressionScheme nodeIDCompressionScheme)
        : BaseLists{fname, nodeIDCompressionScheme.getNumTotalBytes(),
              make_shared<AdjListHeaders>(fname), bufferManager},
          nodeIDCompressionScheme{nodeIDCompressionScheme} {};

    shared_ptr<AdjListHeaders> getHeaders() { return headers; };

private:
    NodeIDCompressionScheme nodeIDCompressionScheme;
};

typedef Lists<int32_t> RelPropertyListsInt;
typedef Lists<double_t> RelPropertyListsDouble;
typedef Lists<uint8_t> RelPropertyListsBool;
typedef Lists<gf_string_t> RelPropertyListsString;
typedef Lists<nodeID_t> AdjLists;

} // namespace storage
} // namespace graphflow
