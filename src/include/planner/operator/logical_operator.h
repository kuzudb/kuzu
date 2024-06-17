#pragma once

#include "common/cast.h"
#include "planner/operator/schema.h"

namespace kuzu {
namespace planner {

// This ENUM is sorted by alphabetical order.
enum class LogicalOperatorType : uint8_t {
    ACCUMULATE,
    AGGREGATE,
    ALTER,
    ATTACH_DATABASE,
    COPY_FROM,
    COPY_TO,
    CREATE_MACRO,
    CREATE_SEQUENCE,
    CREATE_TABLE,
    CREATE_TYPE,
    CROSS_PRODUCT,
    DELETE,
    DETACH_DATABASE,
    DISTINCT,
    DROP_SEQUENCE,
    DROP_TABLE,
    DUMMY_SCAN,
    EMPTY_RESULT,
    EXPLAIN,
    EXPRESSIONS_SCAN,
    EXTEND,
    EXTENSION,
    EXPORT_DATABASE,
    FILTER,
    FLATTEN,
    GDS_CALL,
    HASH_JOIN,
    IMPORT_DATABASE,
    INDEX_LOOK_UP,
    INTERSECT,
    INSERT,
    LIMIT,
    MARK_ACCUMULATE,
    MERGE,
    MULTIPLICITY_REDUCER,
    NODE_LABEL_FILTER,
    ORDER_BY,
    PARTITIONER,
    PATH_PROPERTY_PROBE,
    PROJECTION,
    RECURSIVE_EXTEND,
    SCAN_FILE,
    SCAN_NODE_TABLE,
    SEMI_MASKER,
    SET_PROPERTY,
    STANDALONE_CALL,
    TABLE_FUNCTION_CALL,
    TRANSACTION,
    UNION_ALL,
    UNWIND,
    USE_DATABASE,
};

class LogicalOperator;
using logical_op_vector_t = std::vector<std::shared_ptr<LogicalOperator>>;

struct LogicalOperatorUtils {
    static std::string logicalOperatorTypeToString(LogicalOperatorType type);
    static bool isUpdate(LogicalOperatorType type);
    static bool isAccHashJoin(const LogicalOperator& op);
};

class LogicalOperator {
public:
    explicit LogicalOperator(LogicalOperatorType operatorType) : operatorType{operatorType} {}
    explicit LogicalOperator(LogicalOperatorType operatorType,
        std::shared_ptr<LogicalOperator> child);
    explicit LogicalOperator(LogicalOperatorType operatorType,
        std::shared_ptr<LogicalOperator> left, std::shared_ptr<LogicalOperator> right);
    explicit LogicalOperator(LogicalOperatorType operatorType, const logical_op_vector_t& children);

    virtual ~LogicalOperator() = default;

    uint32_t getNumChildren() const { return children.size(); }
    std::shared_ptr<LogicalOperator> getChild(uint64_t idx) const { return children[idx]; }
    std::vector<std::shared_ptr<LogicalOperator>> getChildren() const { return children; }
    void setChild(uint64_t idx, std::shared_ptr<LogicalOperator> child) {
        children[idx] = std::move(child);
    }

    // Operator type.
    LogicalOperatorType getOperatorType() const { return operatorType; }
    bool hasUpdateRecursive();

    // Schema
    Schema* getSchema() const { return schema.get(); }
    virtual void computeFactorizedSchema() = 0;
    virtual void computeFlatSchema() = 0;

    // Printing.
    virtual std::string getExpressionsForPrinting() const = 0;
    // Print the sub-plan rooted at this operator.
    virtual std::string toString(uint64_t depth = 0) const;

    // TODO: remove this function once planner do not share operator across plans
    virtual std::unique_ptr<LogicalOperator> copy() = 0;
    static logical_op_vector_t copy(const logical_op_vector_t& ops);

    template<class TARGET>
    const TARGET& constCast() const {
        return common::ku_dynamic_cast<const LogicalOperator&, const TARGET&>(*this);
    }
    template<class TARGET>
    TARGET& cast() {
        return common::ku_dynamic_cast<LogicalOperator&, TARGET&>(*this);
    }
    template<class TARGET>
    const TARGET* constPtrCast() const {
        return common::ku_dynamic_cast<const LogicalOperator*, const TARGET*>(this);
    }
    template<class TARGET>
    TARGET* ptrCast() {
        return common::ku_dynamic_cast<LogicalOperator*, TARGET*>(this);
    }

protected:
    void createEmptySchema() { schema = std::make_unique<Schema>(); }
    void copyChildSchema(uint32_t idx) { schema = children[idx]->getSchema()->copy(); }

protected:
    LogicalOperatorType operatorType;
    std::unique_ptr<Schema> schema;
    logical_op_vector_t children;
};

} // namespace planner
} // namespace kuzu
