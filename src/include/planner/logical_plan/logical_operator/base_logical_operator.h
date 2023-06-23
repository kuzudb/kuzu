#pragma once

#include "schema.h"

namespace kuzu {
namespace planner {

enum class LogicalOperatorType : uint8_t {
    ACCUMULATE,
    ADD_PROPERTY,
    AGGREGATE,
    CALL,
    COPY,
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
    PATH_PROPERTY_PROBE,
    PROJECTION,
    RECURSIVE_EXTEND,
    RENAME_TABLE,
    RENAME_PROPERTY,
    SCAN_FRONTIER,
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
    explicit LogicalOperator(
        LogicalOperatorType operatorType, std::shared_ptr<LogicalOperator> child);
    // Binary operator.
    explicit LogicalOperator(LogicalOperatorType operatorType,
        std::shared_ptr<LogicalOperator> left, std::shared_ptr<LogicalOperator> right);
    explicit LogicalOperator(LogicalOperatorType operatorType,
        const std::vector<std::shared_ptr<LogicalOperator>>& children);

    virtual ~LogicalOperator() = default;

    inline uint32_t getNumChildren() const { return children.size(); }

    inline std::shared_ptr<LogicalOperator> getChild(uint64_t idx) const { return children[idx]; }
    inline std::vector<std::shared_ptr<LogicalOperator>> getChildren() const { return children; }
    inline void setChild(uint64_t idx, std::shared_ptr<LogicalOperator> child) {
        children[idx] = std::move(child);
    }
    inline void setChildren(std::vector<std::shared_ptr<LogicalOperator>> children_) {
        children = std::move(children_);
    }

    inline LogicalOperatorType getOperatorType() const { return operatorType; }

    inline Schema* getSchema() const { return schema.get(); }
    virtual void computeFactorizedSchema() = 0;
    virtual void computeFlatSchema() = 0;

    virtual std::string getExpressionsForPrinting() const = 0;

    // Print the sub-plan rooted at this operator.
    virtual std::string toString(uint64_t depth = 0) const;

    // TODO: remove this function once planner do not share operator across plans
    virtual std::unique_ptr<LogicalOperator> copy() = 0;

protected:
    inline void createEmptySchema() { schema = std::make_unique<Schema>(); }
    inline void copyChildSchema(uint32_t idx) { schema = children[idx]->getSchema()->copy(); }

protected:
    LogicalOperatorType operatorType;
    std::unique_ptr<Schema> schema;
    std::vector<std::shared_ptr<LogicalOperator>> children;
};

} // namespace planner
} // namespace kuzu
