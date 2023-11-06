#pragma once

#include "planner/operator/schema.h"

namespace kuzu {
namespace planner {

enum class LogicalOperatorType : uint8_t {
    ACCUMULATE,
    AGGREGATE,
    ALTER,
    COMMENT_ON,
    COPY_FROM,
    COPY_TO,
    CREATE_MACRO,
    CREATE_TABLE,
    CROSS_PRODUCT,
    DELETE_NODE,
    DELETE_REL,
    DISTINCT,
    DROP_TABLE,
    DUMMY_SCAN,
    EXPLAIN,
    EXPRESSIONS_SCAN,
    EXTEND,
    FILL_TABLE_ID,
    FILTER,
    FLATTEN,
    HASH_JOIN,
    IN_QUERY_CALL,
    INDEX_SCAN_NODE,
    INTERSECT,
    INSERT_NODE,
    INSERT_REL,
    LIMIT,
    MERGE,
    MULTIPLICITY_REDUCER,
    NODE_LABEL_FILTER,
    ORDER_BY,
    PARTITIONER,
    PATH_PROPERTY_PROBE,
    PROJECTION,
    RECURSIVE_EXTEND,
    SCAN_FILE,
    SCAN_FRONTIER,
    SCAN_INTERNAL_ID,
    SCAN_NODE_PROPERTY,
    SEMI_MASKER,
    SET_NODE_PROPERTY,
    SET_REL_PROPERTY,
    STANDALONE_CALL,
    TRANSACTION,
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
