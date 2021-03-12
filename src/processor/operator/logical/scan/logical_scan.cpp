#include "src/processor/include/operator/logical/scan/logical_scan.h"

#include "src/processor/include/operator/physical/scan/physical_scan.h"

namespace graphflow {
namespace processor {

LogicalScan::LogicalScan(FileDeserHelper& fdsh)
    : nodeVarName{*fdsh.readString()}, label{*fdsh.readString()} {}

unique_ptr<Operator> LogicalScan::mapToPhysical(
    const Graph& graph, PhysicalOperatorsInfo& physicalOperatorInfo) {
    auto& catalog = graph.getCatalog();
    auto labelFromStr = catalog.getNodeLabelFromString(label.c_str());
    auto morsel = make_shared<MorselDesc>(graph.getNumNodes(labelFromStr));
    physicalOperatorInfo.put(nodeVarName, 0 /* dataChunkPos */, 0 /* valueVectorPos */);
    return make_unique<PhysicalScan>(morsel);
}

void LogicalScan::serialize(FileSerHelper& fsh) {
    fsh.writeString(typeid(LogicalScan).name());
    fsh.writeString(nodeVarName);
    fsh.writeString(label);
}

} // namespace processor
} // namespace graphflow
