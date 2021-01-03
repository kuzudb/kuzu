#pragma once

#include <memory>

#include "src/common/include/file_ser_deser_helper.h"
#include "src/processor/include/operator/column_reader/adj_column_extend.h"
#include "src/processor/include/operator/column_reader/node_property_column_reader.h"
#include "src/processor/include/operator/column_reader/rel_property_column_reader.h"
#include "src/processor/include/operator/list_reader/adj_list_extend.h"
#include "src/processor/include/operator/list_reader/rel_property_list_reader.h"
#include "src/processor/include/operator/operator.h"
#include "src/processor/include/operator/scan/scan.h"
#include "src/processor/include/operator/sink/sink.h"

using namespace std;

namespace graphflow {
namespace processor {

static unique_ptr<Operator> deserializeOperator(FileDeserHelper& fdsh) {
    string typeIDStr = *fdsh.readString();

    // Scans
    if (0 == string(typeid(ScanSingleLabel).name()).compare(typeIDStr)) {
        return make_unique<ScanSingleLabel>(fdsh);
    }
    if (0 == string(typeid(ScanMultiLabel).name()).compare(typeIDStr)) {
        return make_unique<ScanMultiLabel>(fdsh);
    }

    // ColumnReaders
    if (0 == string(typeid(NodePropertyColumnReader).name()).compare(typeIDStr)) {
        return make_unique<NodePropertyColumnReader>(fdsh);
    }
    if (0 == string(typeid(RelPropertyColumnReader).name()).compare(typeIDStr)) {
        return make_unique<RelPropertyColumnReader>(fdsh);
    }
    if (0 == string(typeid(AdjColumnExtend).name()).compare(typeIDStr)) {
        return make_unique<AdjColumnExtend>(fdsh);
    }

    // ListsReaders
    if (0 == string(typeid(AdjListExtend).name()).compare(typeIDStr)) {
        return make_unique<AdjListExtend>(fdsh);
    }
    if (0 == string(typeid(RelPropertyListReader).name()).compare(typeIDStr)) {
        return make_unique<RelPropertyListReader>(fdsh);
    }

    // Sink
    if (0 == string(typeid(Sink).name()).compare(typeIDStr)) {
        return make_unique<Sink>(fdsh);
    }

    throw invalid_argument("Unrecognized typeIDStr to deserialize. typeIDStr: " + typeIDStr);
}

} // namespace processor
} // namespace graphflow
