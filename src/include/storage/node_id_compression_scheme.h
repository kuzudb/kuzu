#pragma once

#include <unordered_set>

#include "common/types/types_include.h"

namespace kuzu {
namespace storage {

class NodeIDCompressionScheme {

public:
    NodeIDCompressionScheme() : commonTableID{common::INVALID_TABLE_ID} {}
    explicit NodeIDCompressionScheme(const std::unordered_set<common::table_id_t>& nbrNodeTableIDs)
        : commonTableID{
              nbrNodeTableIDs.size() == 1 ? *nbrNodeTableIDs.begin() : common::INVALID_TABLE_ID} {}

    inline uint64_t getNumBytesForNodeIDAfterCompression() const {
        return commonTableID == common::INVALID_TABLE_ID ?
                   common::Types::getDataTypeSize(common::INTERNAL_ID) :
                   sizeof(common::offset_t);
    }

    void readNodeID(uint8_t* data, common::nodeID_t* nodeID) const;
    void writeNodeID(uint8_t* data, const common::nodeID_t& nodeID) const;

private:
    common::table_id_t commonTableID;
};

} // namespace storage
} // namespace kuzu
