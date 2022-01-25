#pragma once

#include "src/processor/include/physical_plan/operator/sink.h"
#include "src/processor/include/physical_plan/result/factorized_table.h"

namespace graphflow {
namespace processor {

class SharedQueryResults {
public:
    inline void appendQueryResult(shared_ptr<FactorizedTable> queryResult) {
        lock_guard<mutex> lck{sharedQueryResultMutex};
        queryResults.emplace_back(queryResult);
    }

    inline void mergeQueryResults() {
        lock_guard<mutex> lck{sharedQueryResultMutex};
        for (auto i = 1u; i < queryResults.size(); i++) {
            queryResults[0]->merge(*queryResults[i]);
        }
    }

    inline shared_ptr<FactorizedTable> getFinalizedQueryResult() {
        lock_guard<mutex> lck{sharedQueryResultMutex};
        return queryResults[0];
    }

private:
    mutex sharedQueryResultMutex;
    vector<shared_ptr<FactorizedTable>> queryResults;
};

class ResultCollector : public Sink {
public:
    ResultCollector(vector<pair<DataPos, bool>> vectorsToCollectInfo,
        shared_ptr<SharedQueryResults> sharedQueryResults, unique_ptr<PhysicalOperator> child,
        ExecutionContext& context, uint32_t id);

    PhysicalOperatorType getOperatorType() override { return RESULT_COLLECTOR; }

    shared_ptr<ResultSet> initResultSet() override;

    void execute() override;

    void finalize() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ResultCollector>(
            vectorsToCollectInfo, sharedQueryResults, children[0]->clone(), context, id);
    }

    inline shared_ptr<FactorizedTable> getFinalizedQueryResult() {
        return sharedQueryResults->getFinalizedQueryResult();
    }

private:
    shared_ptr<FactorizedTable> localQueryResult;
    vector<shared_ptr<ValueVector>> vectorsToCollect;
    vector<pair<DataPos, bool>> vectorsToCollectInfo;
    shared_ptr<SharedQueryResults> sharedQueryResults;
};

} // namespace processor
} // namespace graphflow
