#include "planner/join_order/join_tree.h"

namespace kuzu {
namespace planner {

bool ExtraScanTreeNodeInfo::isSingleRel() const {
    return nodeInfo == nullptr && relInfos.size() == 1;
}

bool JoinTree::isSingleRel() const {
    if (root->type != JoinNodeType::REL_SCAN) {
        return false;
    }
    return root->extraInfo->constCast<ExtraScanTreeNodeInfo>().isSingleRel();
}

void ExtraScanTreeNodeInfo::merge(const ExtraScanTreeNodeInfo& other) {
    KU_ASSERT(other.isSingleRel());
    relInfos.push_back(other.relInfos[0]);
}

}
}
