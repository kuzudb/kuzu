#include "processor/operator/gds_call_shared_state.h"

#include "graph/on_disk_graph.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

void GDSCallSharedState::setGraphNodeMask(std::unique_ptr<NodeOffsetMaskMap> maskMap) {
    auto onDiskGraph = ku_dynamic_cast<graph::OnDiskGraph*>(graph.get());
    onDiskGraph->setNodeOffsetMask(maskMap.get());
    graphNodeMask = std::move(maskMap);
}

} // namespace processor
} // namespace kuzu
