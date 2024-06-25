#include "planner/join_order/join_tree.h"

namespace kuzu {
namespace planner {

void ExtraScanTreeNodeInfo::merge(const ExtraScanTreeNodeInfo& other) {
    KU_ASSERT(other.nodeInfo == nullptr && other.relInfos.size() == 1);
    relInfos.push_back(other.relInfos[0]);
}

} // namespace planner
} // namespace kuzu
