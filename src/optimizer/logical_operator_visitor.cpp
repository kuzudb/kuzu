#include "optimizer/logical_operator_visitor.h"

using namespace kuzu::planner;

namespace kuzu {
namespace optimizer {

void LogicalOperatorVisitor::visitOperatorSwitch(planner::LogicalOperator* op) {
    switch (op->getOperatorType()) {
    case LogicalOperatorType::FLATTEN: {
        visitFlatten(op);
    } break;
    case LogicalOperatorType::EMPTY_RESULT: {
        visitEmptyResult(op);
    } break;
    case LogicalOperatorType::EXPRESSIONS_SCAN: {
        visitExpressionsScan(op);
    } break;
    case LogicalOperatorType::SCAN_INTERNAL_ID: {
        visitScanInternalID(op);
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
    case LogicalOperatorType::LIMIT: {
        visitLimit(op);
    } break;
    case LogicalOperatorType::ACCUMULATE: {
        visitAccumulate(op);
    } break;
    case LogicalOperatorType::MARK_ACCUMULATE: {
        visitMarkAccumulate(op);
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
    case LogicalOperatorType::DELETE: {
        visitDelete(op);
    } break;
    case LogicalOperatorType::INSERT: {
        visitInsert(op);
    } break;
    case LogicalOperatorType::MERGE: {
        visitMerge(op);
    } break;
    case LogicalOperatorType::COPY_TO: {
        visitCopyTo(op);
    } break;
    case LogicalOperatorType::COPY_FROM: {
        visitCopyFrom(op);
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
    case LogicalOperatorType::EMPTY_RESULT: {
        return visitEmptyResultReplace(op);
    }
    case LogicalOperatorType::EXPRESSIONS_SCAN: {
        return visitExpressionsScanReplace(op);
    }
    case LogicalOperatorType::SCAN_INTERNAL_ID: {
        return visitScanInternalIDReplace(op);
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
    case LogicalOperatorType::LIMIT: {
        return visitLimitReplace(op);
    }
    case LogicalOperatorType::ACCUMULATE: {
        return visitAccumulateReplace(op);
    }
    case LogicalOperatorType::MARK_ACCUMULATE: {
        return visitMarkAccumulateReplace(op);
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
    case LogicalOperatorType::DELETE: {
        return visitDeleteReplace(op);
    }
    case LogicalOperatorType::INSERT: {
        return visitInsertReplace(op);
    }
    case LogicalOperatorType::MERGE: {
        return visitMergeReplace(op);
    }
    case LogicalOperatorType::COPY_TO: {
        return visitCopyToReplace(op);
    }
    case LogicalOperatorType::COPY_FROM: {
        return visitCopyFromReplace(op);
    }
    default:
        return op;
    }
}

} // namespace optimizer
} // namespace kuzu
