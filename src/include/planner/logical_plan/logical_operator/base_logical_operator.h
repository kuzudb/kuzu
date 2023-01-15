#pragma once

#include "schema.h"

using namespace std;

namespace kuzu {
namespace planner {

enum class LogicalOperatorType : uint8_t {
    ACCUMULATE,
    ADD_PROPERTY,
    AGGREGATE,
    COPY_CSV,
    CREATE_NODE,
    CREATE_REL,
    CREATE_NODE_TABLE,
    CREATE_REL_TABLE,
    CROSS_PRODUCT,
    DELETE_NODE,
    DELETE_REL,
    DISTINCT,
    DROP_PROPERTY,
    DROP_TABLE,
    EXPRESSIONS_SCAN,
    EXTEND,
    FILTER,
    FLATTEN,
    FTABLE_SCAN,
    HASH_JOIN,
    INTERSECT,
    LIMIT,
    MULTIPLICITY_REDUCER,
    ORDER_BY,
    PROJECTION,
    SCAN_NODE,
    INDEX_SCAN_NODE,
    SCAN_NODE_PROPERTY,
    SEMI_MASKER,
    SET_NODE_PROPERTY,
    SET_REL_PROPERTY,
    SKIP,
    UNION_ALL,
    UNWIND,
};

class LogicalOperatorUtils {
public:
    static std::string logicalOperatorTypeToString(LogicalOperatorType type);
};

class LogicalOperator {
public:
    // Leaf operator.
    explicit LogicalOperator(LogicalOperatorType operatorType) : operatorType{operatorType} {}
    // Unary operator.
    explicit LogicalOperator(LogicalOperatorType operatorType, shared_ptr<LogicalOperator> child);
    // Binary operator.
    explicit LogicalOperator(LogicalOperatorType operatorType, shared_ptr<LogicalOperator> left,
        shared_ptr<LogicalOperator> right);
    explicit LogicalOperator(
        LogicalOperatorType operatorType, vector<shared_ptr<LogicalOperator>> children);

    virtual ~LogicalOperator() = default;

    inline uint32_t getNumChildren() const { return children.size(); }

    // Used for operators with more than two children e.g. Union
    inline void addChild(shared_ptr<LogicalOperator> op) { children.push_back(std::move(op)); }
    inline shared_ptr<LogicalOperator> getChild(uint64_t idx) const { return children[idx]; }

    inline LogicalOperatorType getOperatorType() const { return operatorType; }

    inline Schema* getSchema() const { return schema.get(); }
    void computeSchemaRecursive();
    virtual void computeSchema() = 0;

    virtual string getExpressionsForPrinting() const = 0;

    // Print the sub-plan rooted at this operator.
    virtual string toString(uint64_t depth = 0) const;

    // TODO: remove this function once planner do not share operator across plans
    virtual unique_ptr<LogicalOperator> copy() = 0;

protected:
    inline void createEmptySchema() { schema = make_unique<Schema>(); }
    inline void copyChildSchema(uint32_t idx) { schema = children[idx]->getSchema()->copy(); }

protected:
    LogicalOperatorType operatorType;
    unique_ptr<Schema> schema;
    vector<shared_ptr<LogicalOperator>> children;
};

} // namespace planner
} // namespace kuzu
