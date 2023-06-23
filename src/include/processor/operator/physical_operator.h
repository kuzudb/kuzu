#pragma once

#include "processor/data_pos.h"
#include "processor/execution_context.h"
#include "processor/result/result_set.h"
#include "storage/buffer_manager/buffer_manager.h"

namespace kuzu {
namespace processor {

enum class PhysicalOperatorType : uint8_t {
    ADD_PROPERTY,
    AGGREGATE,
    AGGREGATE_SCAN,
    CALL,
    COPY_NODE,
    COPY_REL,
    COPY_NPY,
    READ_CSV,
    READ_PARQUET,
    CREATE_NODE,
    CREATE_NODE_TABLE,
    CREATE_REL,
    CREATE_REL_TABLE,
    CROSS_PRODUCT,
    DELETE_NODE,
    DELETE_REL,
    DROP_PROPERTY,
    DROP_TABLE,
    FACTORIZED_TABLE_SCAN,
    FILTER,
    FLATTEN,
    GENERIC_SCAN_REL_TABLES,
    HASH_JOIN_BUILD,
    HASH_JOIN_PROBE,
    INDEX_SCAN,
    INTERSECT_BUILD,
    INTERSECT,
    LIMIT,
    MULTIPLICITY_REDUCER,
    PATH_PROPERTY_PROBE,
    PROJECTION,
    RECURSIVE_JOIN,
    RENAME_PROPERTY,
    RENAME_TABLE,
    RESULT_COLLECTOR,
    SCAN_FRONTIER,
    SCAN_NODE_ID,
    SCAN_NODE_PROPERTY,
    SCAN_REL_PROPERTY,
    SCAN_REL_TABLE_COLUMNS,
    SCAN_REL_TABLE_LISTS,
    SEMI_MASKER,
    SET_NODE_PROPERTY,
    SET_REL_PROPERTY,
    SIMPLE_RECURSIVE_JOIN,
    SKIP,
    ORDER_BY,
    ORDER_BY_MERGE,
    ORDER_BY_SCAN,
    UNION_ALL_SCAN,
    UNWIND,
    VAR_LENGTH_ADJ_LIST_EXTEND,
    VAR_LENGTH_COLUMN_EXTEND,
};

class PhysicalOperatorUtils {
public:
    static std::string operatorTypeToString(PhysicalOperatorType operatorType);
};

struct OperatorMetrics {

public:
    OperatorMetrics(common::TimeMetric& executionTime, common::NumericMetric& numOutputTuple)
        : executionTime{executionTime}, numOutputTuple{numOutputTuple} {}

public:
    common::TimeMetric& executionTime;
    common::NumericMetric& numOutputTuple;
};

class PhysicalOperator {
public:
    // Leaf operator
    PhysicalOperator(PhysicalOperatorType operatorType, uint32_t id, std::string paramsString)
        : operatorType{operatorType}, id{id}, transaction{nullptr}, paramsString{
                                                                        std::move(paramsString)} {}
    // Unary operator
    PhysicalOperator(PhysicalOperatorType operatorType, std::unique_ptr<PhysicalOperator> child,
        uint32_t id, const std::string& paramsString);
    // Binary operator
    PhysicalOperator(PhysicalOperatorType operatorType, std::unique_ptr<PhysicalOperator> left,
        std::unique_ptr<PhysicalOperator> right, uint32_t id, const std::string& paramsString);
    // This constructor is used by UnionAllScan only since it may have multiple children.
    PhysicalOperator(PhysicalOperatorType operatorType,
        std::vector<std::unique_ptr<PhysicalOperator>> children, uint32_t id,
        const std::string& paramsString);

    virtual ~PhysicalOperator() = default;

    inline uint32_t getOperatorID() const { return id; }

    inline PhysicalOperatorType getOperatorType() const { return operatorType; }

    inline virtual bool isSource() const { return false; }
    inline virtual bool isSink() const { return false; }

    inline void addChild(std::unique_ptr<PhysicalOperator> op) {
        children.push_back(std::move(op));
    }
    inline PhysicalOperator* getChild(uint64_t idx) const { return children[idx].get(); }
    inline uint64_t getNumChildren() const { return children.size(); }
    std::unique_ptr<PhysicalOperator> moveUnaryChild();

    inline std::string getParamsString() const { return paramsString; }

    // Global state is initialized once.
    void initGlobalState(ExecutionContext* context);
    // Local state is initialized for each thread.
    void initLocalState(ResultSet* resultSet, ExecutionContext* context);

    inline bool getNextTuple(ExecutionContext* context) {
        if (context->clientContext->isInterrupted()) {
            throw common::InterruptException{};
        }
        metrics->executionTime.start();
        auto result = getNextTuplesInternal(context);
        metrics->executionTime.stop();
        return result;
    }

    std::unordered_map<std::string, std::string> getProfilerKeyValAttributes(
        common::Profiler& profiler) const;
    std::vector<std::string> getProfilerAttributes(common::Profiler& profiler) const;

    virtual std::unique_ptr<PhysicalOperator> clone() = 0;

protected:
    virtual void initGlobalStateInternal(ExecutionContext* context) {}
    virtual void initLocalStateInternal(ResultSet* resultSet_, ExecutionContext* context) {}
    // Return false if no more tuples to pull, otherwise return true
    virtual bool getNextTuplesInternal(ExecutionContext* context) = 0;

    inline std::string getTimeMetricKey() const { return "time-" + std::to_string(id); }
    inline std::string getNumTupleMetricKey() const { return "numTuple-" + std::to_string(id); }

    void registerProfilingMetrics(common::Profiler* profiler);

    double getExecutionTime(common::Profiler& profiler) const;
    uint64_t getNumOutputTuples(common::Profiler& profiler) const;

protected:
    uint32_t id;
    std::unique_ptr<OperatorMetrics> metrics;
    PhysicalOperatorType operatorType;

    std::vector<std::unique_ptr<PhysicalOperator>> children;
    transaction::Transaction* transaction;
    ResultSet* resultSet;

    std::string paramsString;
};

} // namespace processor
} // namespace kuzu
