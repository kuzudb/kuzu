#pragma once

#include <unordered_set>

#include "common/types/types_include.h"

namespace kuzu {
namespace common {

class NodeIDCompressionScheme {

public:
    NodeIDCompressionScheme() : commonTableID{INVALID_TABLE_ID} {}
    explicit NodeIDCompressionScheme(const unordered_set<table_id_t>& nbrNodeTableIDs)
        : commonTableID{nbrNodeTableIDs.size() == 1 ? *nbrNodeTableIDs.begin() : INVALID_TABLE_ID} {
    }

    inline uint64_t getNumBytesForNodeIDAfterCompression() const {
        return commonTableID == INVALID_TABLE_ID ? Types::getDataTypeSize(NODE_ID) :
                                                   sizeof(node_offset_t);
    }

    void readNodeID(uint8_t* data, nodeID_t* nodeID) const;
    void writeNodeID(uint8_t* data, const nodeID_t& nodeID) const;

private:
    table_id_t commonTableID;
};

} // namespace common
} // namespace kuzu
