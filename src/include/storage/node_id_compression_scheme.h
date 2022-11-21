#pragma once

#include <unordered_set>

#include "common/types/types_include.h"

namespace kuzu {
namespace common {

class NodeIDCompressionScheme {

public:
    NodeIDCompressionScheme() : commonTableID{UINT64_MAX} {}
    explicit NodeIDCompressionScheme(const unordered_set<table_id_t>& nbrNodeTableIDs)
        : commonTableID{nbrNodeTableIDs.size() == 1 ? *nbrNodeTableIDs.begin() : UINT64_MAX} {}

    inline uint64_t getNumBytesForNodeIDAfterCompression() const {
        return commonTableID == UINT64_MAX ? Types::getDataTypeSize(NODE_ID) :
                                             sizeof(node_offset_t);
    }
    inline table_id_t getCommonTableID() const { return commonTableID; }

    void readNodeID(uint8_t* data, nodeID_t* nodeID) const;
    void writeNodeID(uint8_t* data, const nodeID_t& nodeID) const;

private:
    table_id_t commonTableID;
};

} // namespace common
} // namespace kuzu
