#pragma once

#include "src/common/include/file_ser_deser_helper.h"
#include "src/processor/include/operator/logical/extend/logical_extend.h"
#include "src/processor/include/operator/logical/property_reader/logical_node_property_reader.h"
#include "src/processor/include/operator/logical/property_reader/logical_rel_property_reader.h"
#include "src/processor/include/operator/logical/scan/logical_scan.h"

using namespace std;

namespace graphflow {
namespace processor {

static unique_ptr<LogicalOperator> deserializeOperator(FileDeserHelper& fdsh) {
    string typeIDStr = *fdsh.readString();

    // Scan
    if (string(typeid(LogicalScan).name()) == typeIDStr) {
        return make_unique<LogicalScan>(fdsh);
    }
    // Extend
    if (string(typeid(LogicalExtend).name()) == typeIDStr) {
        return make_unique<LogicalExtend>(fdsh);
    }
    // Node Property Reader
    if (string(typeid(LogicalNodePropertyReader).name()) == typeIDStr) {
        return make_unique<LogicalNodePropertyReader>(fdsh);
    }
    // Rel Property Reader
    if (string(typeid(LogicalRelPropertyReader).name()) == typeIDStr) {
        return make_unique<LogicalRelPropertyReader>(fdsh);
    }
    throw invalid_argument("Unrecognized typeIDStr to deserialize. typeIDStr: " + typeIDStr);
}

} // namespace processor
} // namespace graphflow
