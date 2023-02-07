#include "storage/node_id_compression_scheme.h"

namespace kuzu {
namespace storage {

void NodeIDCompressionScheme::readNodeID(uint8_t* data, common::nodeID_t* nodeID) const {
    if (commonTableID == common::INVALID_TABLE_ID) {
        memcpy(&*nodeID, data, sizeof(common::nodeID_t));
    } else {
        nodeID->tableID = commonTableID;
        memcpy(&nodeID->offset, data, sizeof(common::offset_t));
    }
}

void NodeIDCompressionScheme::writeNodeID(uint8_t* data, const common::nodeID_t& nodeID) const {
    if (commonTableID == common::INVALID_TABLE_ID) {
        memcpy(data, &nodeID, sizeof(common::nodeID_t));
    } else {
        memcpy(data, &nodeID.offset, sizeof(common::offset_t));
    }
}

} // namespace storage
} // namespace kuzu
