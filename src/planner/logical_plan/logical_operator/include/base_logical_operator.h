#pragma once

#include "schema.h"

using namespace std;

namespace graphflow {
namespace planner {

enum LogicalOperatorType : uint8_t {
    LOGICAL_SCAN_NODE_ID,
    LOGICAL_SELECT_SCAN,
    LOGICAL_EXTEND,
    LOGICAL_FLATTEN,
    LOGICAL_FILTER,
    LOGICAL_INTERSECT,
    LOGICAL_PROJECTION,
    LOGICAL_SCAN_NODE_PROPERTY,
    LOGICAL_SCAN_REL_PROPERTY,
    LOGICAL_HASH_JOIN,
    LOGICAL_MULTIPLICITY_REDUCER,
    LOGICAL_LIMIT,
    LOGICAL_SKIP,
    LOGICAL_AGGREGATE,
    LOGICAL_ORDER_BY,
    LOGICAL_EXISTS,
    LOGICAL_LEFT_NESTED_LOOP_JOIN,
    LOGICAL_UNION_ALL,
    LOGICAL_DISTINCT,
    LOGICAL_CREATE,
    LOGICAL_SET,
    LOGICAL_DELETE,
    LOGICAL_ACCUMULATE,
    LOGICAL_STATIC_TABLE_SCAN,
    LOGICAL_CREATE_NODE_TABLE,
    LOGICAL_CREATE_REL_TABLE,
    LOGICAL_COPY_CSV,
    LOGICAL_DROP_TABLE,
};

const string LogicalOperatorTypeNames[] = {"LOGICAL_SCAN_NODE_ID", "LOGICAL_SELECT_SCAN",
    "LOGICAL_EXTEND", "LOGICAL_FLATTEN", "LOGICAL_FILTER", "LOGICAL_INTERSECT",
    "LOGICAL_PROJECTION", "LOGICAL_SCAN_NODE_PROPERTY", "LOGICAL_SCAN_REL_PROPERTY",
    "LOGICAL_HASH_JOIN", "LOGICAL_MULTIPLICITY_REDUCER", "LOGICAL_LIMIT", "LOGICAL_SKIP",
    "LOGICAL_AGGREGATE", "LOGICAL_ORDER_BY", "LOGICAL_EXISTS", "LOGICAL_LEFT_NESTED_LOOP_JOIN",
    "LOGICAL_UNION_ALL", "LOGICAL_DISTINCT", "LOGICAL_CREATE", "LOGICAL_SET", "LOGICAL_DELETE",
    "LOGICAL_ACCUMULATE", "LOGICAL_STATIC_TABLE_SCAN", "LOGICAL_CREATE_NODE_TABLE",
    "LOGICAL_CREATE_REL_TABLE", "LOGICAL_COPY_CSV", "LOGICAL_DROP_TABLE"};

class LogicalOperator {
public:
    // Leaf operator.
    LogicalOperator() = default;
    // Unary operator.
    LogicalOperator(shared_ptr<LogicalOperator> child);
    // Binary operator.
    LogicalOperator(shared_ptr<LogicalOperator> left, shared_ptr<LogicalOperator> right);
    LogicalOperator(vector<shared_ptr<LogicalOperator>> children);

    virtual ~LogicalOperator() = default;

    inline uint32_t getNumChildren() const { return children.size(); }

    // Used for operators with more than two children e.g. Union
    inline void addChild(shared_ptr<LogicalOperator> op) { children.push_back(move(op)); }

    inline shared_ptr<LogicalOperator> getChild(uint64_t idx) const { return children[idx]; }

    virtual LogicalOperatorType getLogicalOperatorType() const = 0;

    virtual string getExpressionsForPrinting() const = 0;

    bool descendantsContainType(const unordered_set<LogicalOperatorType>& types) const;

    // Print the sub-plan rooted at this operator.
    virtual string toString(uint64_t depth = 0) const;

    // TODO: remove this function once planner do not share operator across plans
    virtual unique_ptr<LogicalOperator> copy() = 0;

protected:
    vector<shared_ptr<LogicalOperator>> children;
};

} // namespace planner
} // namespace graphflow
