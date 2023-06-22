#include "optimizer/logical_operator_visitor.h"

using namespace kuzu::planner;

namespace kuzu {
namespace optimizer {

void LogicalOperatorVisitor::visitOperatorSwitch(planner::LogicalOperator* op) {
    switch (op->getOperatorType()) {
    case LogicalOperatorType::FLATTEN: {
        visitFlatten(op);
    } break;
    case LogicalOperatorType::SCAN_NODE: {
        visitScanNode(op);
    } break;
    case LogicalOperatorType::INDEX_SCAN_NODE: {
        visitIndexScanNode(op);
    } break;
    case LogicalOperatorType::EXTEND: {
        visitExtend(op);
    } break;
    case LogicalOperatorType::RECURSIVE_EXTEND: {
        visitRecursiveExtend(op);
    } break;
    case LogicalOperatorType::PATH_PROPERTY_PROBE: {
        visitPathPropertyProbe(op);
    } break;
    case LogicalOperatorType::HASH_JOIN: {
        visitHashJoin(op);
    } break;
    case LogicalOperatorType::INTERSECT: {
        visitIntersect(op);
    } break;
    case LogicalOperatorType::PROJECTION: {
        visitProjection(op);
    } break;
    case LogicalOperatorType::AGGREGATE: {
        visitAggregate(op);
    } break;
    case LogicalOperatorType::ORDER_BY: {
        visitOrderBy(op);
    } break;
    case LogicalOperatorType::SKIP: {
        visitSkip(op);
    } break;
    case LogicalOperatorType::LIMIT: {
        visitLimit(op);
    } break;
    case LogicalOperatorType::ACCUMULATE: {
        visitAccumulate(op);
    } break;
    case LogicalOperatorType::DISTINCT: {
        visitDistinct(op);
    } break;
    case LogicalOperatorType::UNWIND: {
        visitUnwind(op);
    } break;
    case LogicalOperatorType::UNION_ALL: {
        visitUnion(op);
    } break;
    case LogicalOperatorType::FILTER: {
        visitFilter(op);
    } break;
    case LogicalOperatorType::SET_NODE_PROPERTY: {
        visitSetNodeProperty(op);
    } break;
    case LogicalOperatorType::SET_REL_PROPERTY: {
        visitSetRelProperty(op);
    } break;
    case LogicalOperatorType::DELETE_NODE: {
        visitDeleteNode(op);
    } break;
    case LogicalOperatorType::DELETE_REL: {
        visitDeleteRel(op);
    } break;
    case LogicalOperatorType::CREATE_NODE: {
        visitCreateNode(op);
    } break;
    case LogicalOperatorType::CREATE_REL: {
        visitCreateRel(op);
    } break;
    default:
        return;
    }
}

std::shared_ptr<planner::LogicalOperator> LogicalOperatorVisitor::visitOperatorReplaceSwitch(
    std::shared_ptr<planner::LogicalOperator> op) {
    switch (op->getOperatorType()) {
    case LogicalOperatorType::FLATTEN: {
        return visitFlattenReplace(op);
    }
    case LogicalOperatorType::SCAN_NODE: {
        return visitScanNodeReplace(op);
    }
    case LogicalOperatorType::INDEX_SCAN_NODE: {
        return visitIndexScanNodeReplace(op);
    }
    case LogicalOperatorType::EXTEND: {
        return visitExtendReplace(op);
    }
    case LogicalOperatorType::RECURSIVE_EXTEND: {
        return visitRecursiveExtendReplace(op);
    }
    case LogicalOperatorType::PATH_PROPERTY_PROBE: {
        return visitPathPropertyProbeReplace(op);
    }
    case LogicalOperatorType::HASH_JOIN: {
        return visitHashJoinReplace(op);
    }
    case LogicalOperatorType::INTERSECT: {
        return visitIntersectReplace(op);
    }
    case LogicalOperatorType::PROJECTION: {
        return visitProjectionReplace(op);
    }
    case LogicalOperatorType::AGGREGATE: {
        return visitAggregateReplace(op);
    }
    case LogicalOperatorType::ORDER_BY: {
        return visitOrderByReplace(op);
    }
    case LogicalOperatorType::SKIP: {
        return visitSkipReplace(op);
    }
    case LogicalOperatorType::LIMIT: {
        return visitLimitReplace(op);
    }
    case LogicalOperatorType::ACCUMULATE: {
        return visitAccumulateReplace(op);
    }
    case LogicalOperatorType::DISTINCT: {
        return visitDistinctReplace(op);
    }
    case LogicalOperatorType::UNWIND: {
        return visitUnwindReplace(op);
    }
    case LogicalOperatorType::UNION_ALL: {
        return visitUnionReplace(op);
    }
    case LogicalOperatorType::FILTER: {
        return visitFilterReplace(op);
    }
    case LogicalOperatorType::SET_NODE_PROPERTY: {
        return visitSetNodePropertyReplace(op);
    }
    case LogicalOperatorType::SET_REL_PROPERTY: {
        return visitSetRelPropertyReplace(op);
    }
    case LogicalOperatorType::DELETE_NODE: {
        return visitDeleteNodeReplace(op);
    }
    case LogicalOperatorType::DELETE_REL: {
        return visitDeleteRelReplace(op);
    }
    case LogicalOperatorType::CREATE_NODE: {
        return visitCreateNodeReplace(op);
    }
    case LogicalOperatorType::CREATE_REL: {
        return visitCreateRelReplace(op);
    }
    default:
        return op;
    }
}

} // namespace optimizer
} // namespace kuzu
