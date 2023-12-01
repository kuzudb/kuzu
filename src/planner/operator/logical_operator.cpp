#include "planner/operator/logical_operator.h"

using namespace kuzu::common;

namespace kuzu {
namespace planner {

std::string LogicalOperatorUtils::logicalOperatorTypeToString(LogicalOperatorType type) {
    switch (type) {
    case LogicalOperatorType::ACCUMULATE: {
        return "ACCUMULATE";
    }
    case LogicalOperatorType::AGGREGATE: {
        return "AGGREGATE";
    }
    case LogicalOperatorType::ALTER: {
        return "ALTER";
    }
    case LogicalOperatorType::COMMENT_ON: {
        return "COMMENT_ON";
    }
    case LogicalOperatorType::COPY_FROM: {
        return "COPY_FROM";
    }
    case LogicalOperatorType::COPY_TO: {
        return "COPY_TO";
    }
    case LogicalOperatorType::CREATE_MACRO: {
        return "CREATE_MACRO";
    }
    case LogicalOperatorType::CREATE_TABLE: {
        return "CREATE_TABLE";
    }
    case LogicalOperatorType::CROSS_PRODUCT: {
        return "CROSS_PRODUCT";
    }
    case LogicalOperatorType::DELETE_NODE: {
        return "DELETE_NODE";
    }
    case LogicalOperatorType::DELETE_REL: {
        return "DELETE_REL";
    }
    case LogicalOperatorType::DISTINCT: {
        return "DISTINCT";
    }
    case LogicalOperatorType::DROP_TABLE: {
        return "DROP_TABLE";
    }
    case LogicalOperatorType::DUMMY_SCAN: {
        return "DUMMY_SCAN";
    }
    case LogicalOperatorType::EXTEND: {
        return "EXTEND";
    }
    case LogicalOperatorType::EXPRESSIONS_SCAN: {
        return "EXPRESSIONS_SCAN";
    }
    case LogicalOperatorType::EXPLAIN: {
        return "EXPLAIN";
    }
    case LogicalOperatorType::FILL_TABLE_ID: {
        return "FILL_TABLE_ID";
    }
    case LogicalOperatorType::FILTER: {
        return "FILTER";
    }
    case LogicalOperatorType::FLATTEN: {
        return "FLATTEN";
    }
    case LogicalOperatorType::HASH_JOIN: {
        return "HASH_JOIN";
    }
    case LogicalOperatorType::IN_QUERY_CALL: {
        return "IN_QUERY_CALL";
    }
    case LogicalOperatorType::INDEX_SCAN_NODE: {
        return "INDEX_SCAN_NODE";
    }
    case LogicalOperatorType::INTERSECT: {
        return "INTERSECT";
    }
    case LogicalOperatorType::INSERT_NODE: {
        return "INSERT_NODE";
    }
    case LogicalOperatorType::INSERT_REL: {
        return "INSERT_REL";
    }
    case LogicalOperatorType::LIMIT: {
        return "LIMIT";
    }
    case LogicalOperatorType::MERGE: {
        return "MERGE";
    }
    case LogicalOperatorType::MULTIPLICITY_REDUCER: {
        return "MULTIPLICITY_REDUCER";
    }
    case LogicalOperatorType::NODE_LABEL_FILTER: {
        return "NODE_LABEL_FILTER";
    }
    case LogicalOperatorType::ORDER_BY: {
        return "ORDER_BY";
    }
    case LogicalOperatorType::PARTITIONER: {
        return "PARTITIONER";
    }
    case LogicalOperatorType::PATH_PROPERTY_PROBE: {
        return "PATH_PROPERTY_PROBE";
    }
    case LogicalOperatorType::PROJECTION: {
        return "PROJECTION";
    }
    case LogicalOperatorType::RECURSIVE_EXTEND: {
        return "RECURSIVE_EXTEND";
    }
    case LogicalOperatorType::SCAN_FILE: {
        return "SCAN_FILE";
    }
    case LogicalOperatorType::SCAN_FRONTIER: {
        return "SCAN_FRONTIER";
    }
    case LogicalOperatorType::SCAN_INTERNAL_ID: {
        return "SCAN_INTERNAL_ID";
    }
    case LogicalOperatorType::SCAN_NODE_PROPERTY: {
        return "SCAN_NODE_PROPERTY";
    }
    case LogicalOperatorType::SEMI_MASKER: {
        return "SEMI_MASKER";
    }
    case LogicalOperatorType::SET_NODE_PROPERTY: {
        return "SET_NODE_PROPERTY";
    }
    case LogicalOperatorType::SET_REL_PROPERTY: {
        return "SET_REL_PROPERTY";
    }
    case LogicalOperatorType::STANDALONE_CALL: {
        return "STANDALONE_CALL";
    }
    case LogicalOperatorType::TRANSACTION: {
        return "TRANSACTION";
    }
    case LogicalOperatorType::UNION_ALL: {
        return "UNION_ALL";
    }
    case LogicalOperatorType::UNWIND: {
        return "UNWIND";
    }
    default:
        KU_UNREACHABLE;
    }
}

LogicalOperator::LogicalOperator(
    LogicalOperatorType operatorType, std::shared_ptr<LogicalOperator> child)
    : operatorType{operatorType} {
    children.push_back(std::move(child));
}

LogicalOperator::LogicalOperator(LogicalOperatorType operatorType,
    std::shared_ptr<LogicalOperator> left, std::shared_ptr<LogicalOperator> right)
    : operatorType{operatorType} {
    children.push_back(std::move(left));
    children.push_back(std::move(right));
}

LogicalOperator::LogicalOperator(
    LogicalOperatorType operatorType, const std::vector<std::shared_ptr<LogicalOperator>>& children)
    : operatorType{operatorType} {
    for (auto& child : children) {
        this->children.push_back(child);
    }
}

std::string LogicalOperator::toString(uint64_t depth) const {
    auto padding = std::string(depth * 4, ' ');
    std::string result = padding;
    result += LogicalOperatorUtils::logicalOperatorTypeToString(operatorType) + "[" +
              getExpressionsForPrinting() + "]";
    if (children.size() == 1) {
        result += "\n" + children[0]->toString(depth);
    } else {
        for (auto& child : children) {
            result += "\n" + padding + "CHILD:\n" + child->toString(depth + 1);
        }
    }
    return result;
}

} // namespace planner
} // namespace kuzu
