#include "src/storage/include/node_id_compression_scheme.h"

namespace kuzu {
namespace common {

void NodeIDCompressionScheme::readNodeID(uint8_t* data, nodeID_t* nodeID) const {
    if (commonTableID == UINT64_MAX) {
        memcpy(&*nodeID, data, Types::getDataTypeSize(NODE_ID));
    } else {
        nodeID->tableID = commonTableID;
        memcpy(&nodeID->offset, data, sizeof(node_offset_t));
    }
}

void NodeIDCompressionScheme::writeNodeID(uint8_t* data, nodeID_t* nodeID) const {
    if (commonTableID == UINT64_MAX) {
        memcpy(data, nodeID, Types::getDataTypeSize(NODE_ID));
    } else {
        memcpy(data, &nodeID->offset, sizeof(node_offset_t));
    }
}

} // namespace common
} // namespace kuzu
