#include "storage/node_id_compression_scheme.h"

namespace kuzu {
namespace common {

void NodeIDCompressionScheme::readNodeID(uint8_t* data, nodeID_t* nodeID) const {
    if (commonTableID == UINT64_MAX) {
        memcpy(&*nodeID, data, sizeof(nodeID_t));
    } else {
        nodeID->tableID = commonTableID;
        memcpy(&nodeID->offset, data, sizeof(node_offset_t));
    }
}

void NodeIDCompressionScheme::writeNodeID(uint8_t* data, const nodeID_t& nodeID) const {
    if (commonTableID == UINT64_MAX) {
        memcpy(data, &nodeID, sizeof(nodeID_t));
    } else {
        memcpy(data, &nodeID.offset, sizeof(node_offset_t));
    }
}

} // namespace common
} // namespace kuzu
