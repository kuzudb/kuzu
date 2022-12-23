#pragma once

#include "processor/data_pos.h"
#include "processor/execution_context.h"
#include "processor/result/result_set.h"
#include "storage/buffer_manager/buffer_manager.h"

namespace kuzu {
namespace processor {

enum class PhysicalOperatorType : uint8_t {
    AGGREGATE,
    AGGREGATE_SCAN,
    COLUMN_EXTEND,
    COPY_NODE_CSV,
    COPY_REL_CSV,
    CREATE_NODE,
    CREATE_NODE_TABLE,
    CREATE_REL,
    CREATE_REL_TABLE,
    CROSS_PRODUCT,
    DELETE_NODE,
    DELETE_REL,
    DROP_TABLE,
    FACTORIZED_TABLE_SCAN,
    FILTER,
    FLATTEN,
    GENERIC_EXTEND,
    HASH_JOIN_BUILD,
    HASH_JOIN_PROBE,
    INDEX_SCAN,
    INTERSECT_BUILD,
    INTERSECT,
    LIMIT,
    LIST_EXTEND,
    MULTIPLICITY_REDUCER,
    PROJECTION,
    SCAN_REL_PROPERTY,
    RESULT_COLLECTOR,
    SCAN_NODE_ID,
    SCAN_NODE_PROPERTY,
    SEMI_MASKER,
    SET_NODE_PROPERTY,
    SET_REL_PROPERTY,
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
    OperatorMetrics(TimeMetric& executionTime, NumericMetric& numOutputTuple)
        : executionTime{executionTime}, numOutputTuple{numOutputTuple} {}

public:
    TimeMetric& executionTime;
    NumericMetric& numOutputTuple;
};

class PhysicalOperator {
public:
    // Leaf operator
    PhysicalOperator(PhysicalOperatorType operatorType, uint32_t id, string paramsString)
        : operatorType{operatorType}, id{id}, transaction{nullptr}, paramsString{
                                                                        std::move(paramsString)} {}
    // Unary operator
    PhysicalOperator(PhysicalOperatorType operatorType, unique_ptr<PhysicalOperator> child,
        uint32_t id, const string& paramsString);
    // Binary operator
    PhysicalOperator(PhysicalOperatorType operatorType, unique_ptr<PhysicalOperator> left,
        unique_ptr<PhysicalOperator> right, uint32_t id, const string& paramsString);
    // This constructor is used by UnionAllScan only since it may have multiple children.
    PhysicalOperator(PhysicalOperatorType operatorType,
        vector<unique_ptr<PhysicalOperator>> children, uint32_t id, const string& paramsString);

    virtual ~PhysicalOperator() = default;

    inline uint32_t getOperatorID() const { return id; }

    inline PhysicalOperatorType getOperatorType() const { return operatorType; }

    inline virtual bool isSource() const { return false; }
    inline virtual bool isSink() const { return false; }

    inline void addChild(unique_ptr<PhysicalOperator> op) { children.push_back(std::move(op)); }
    inline PhysicalOperator* getChild(uint64_t idx) const { return children[idx].get(); }
    inline uint64_t getNumChildren() const { return children.size(); }
    unique_ptr<PhysicalOperator> moveUnaryChild();

    inline string getParamsString() const { return paramsString; }

    // Global state is initialized once.
    void initGlobalState(ExecutionContext* context);
    // Local state is initialized for each thread.
    void initLocalState(ResultSet* resultSet, ExecutionContext* context);

    inline bool getNextTuple() {
        metrics->executionTime.start();
        auto result = getNextTuplesInternal();
        metrics->executionTime.stop();
        return result;
    }

    unordered_map<string, string> getProfilerKeyValAttributes(Profiler& profiler) const;
    vector<string> getProfilerAttributes(Profiler& profiler) const;

    virtual unique_ptr<PhysicalOperator> clone() = 0;

protected:
    virtual void initGlobalStateInternal(ExecutionContext* context) {}
    virtual void initLocalStateInternal(ResultSet* resultSet_, ExecutionContext* context) {}
    // Return false if no more tuples to pull, otherwise return true
    virtual bool getNextTuplesInternal() = 0;

    inline string getTimeMetricKey() const { return "time-" + to_string(id); }
    inline string getNumTupleMetricKey() const { return "numTuple-" + to_string(id); }

    void registerProfilingMetrics(Profiler* profiler);

    double getExecutionTime(Profiler& profiler) const;
    uint64_t getNumOutputTuples(Profiler& profiler) const;

protected:
    uint32_t id;
    unique_ptr<OperatorMetrics> metrics;
    PhysicalOperatorType operatorType;

    vector<unique_ptr<PhysicalOperator>> children;
    Transaction* transaction;
    ResultSet* resultSet;

    string paramsString;
};

} // namespace processor
} // namespace kuzu
