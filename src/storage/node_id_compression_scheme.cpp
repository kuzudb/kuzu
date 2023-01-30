#include "storage/node_id_compression_scheme.h"

namespace kuzu {
namespace common {

void NodeIDCompressionScheme::readNodeID(uint8_t* data, nodeID_t* nodeID) const {
    if (commonTableID == INVALID_TABLE_ID) {
        memcpy(&*nodeID, data, sizeof(nodeID_t));
    } else {
        nodeID->tableID = commonTableID;
        memcpy(&nodeID->offset, data, sizeof(offset_t));
    }
}

void NodeIDCompressionScheme::writeNodeID(uint8_t* data, const nodeID_t& nodeID) const {
    if (commonTableID == INVALID_TABLE_ID) {
        memcpy(data, &nodeID, sizeof(nodeID_t));
    } else {
        memcpy(data, &nodeID.offset, sizeof(offset_t));
    }
}

} // namespace common
} // namespace kuzu
