#pragma once

#include <queue>

#include "src/processor/include/physical_plan/operator/sink.h"
#include "src/processor/include/physical_plan/result/factorized_table.h"
#include "src/processor/include/physical_plan/result/result_set.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace processor {

struct VarLengthJoinSharedState {
public:
    void appendLocalFactorizedTable(shared_ptr<FactorizedTable> localFactorizedTable) {
        lock_guard<mutex> lck{varLengthJoinSharedStateMutex};
        this->factorizedTable.push_back(move(localFactorizedTable));
    }

private:
    mutex varLengthJoinSharedStateMutex;
    vector<shared_ptr<FactorizedTable>> factorizedTable;
};

class VarLengthJoinSink : public Sink {

public:
    VarLengthJoinSink(vector<DataPos> inputDataPoses, vector<bool> isInputVectorFlat,
        shared_ptr<VarLengthJoinSharedState> varLengthSharedState,
        unique_ptr<PhysicalOperator> child, ExecutionContext& context, uint32_t id)
        : Sink{move(child), context, id}, inputDataPoses{move(inputDataPoses)},
          varLengthSharedState{varLengthSharedState}, isInputVectorFlat{move(isInputVectorFlat)} {}

    shared_ptr<ResultSet> initResultSet() override;
    void execute() override;

    PhysicalOperatorType getOperatorType() override { return VAR_LENGTH_JOIN; }

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<VarLengthJoinSink>(inputDataPoses, isInputVectorFlat,
            varLengthSharedState, children[0]->clone(), context, id);
    }

public:
    vector<DataPos> inputDataPoses;
    vector<bool> isInputVectorFlat;
    vector<shared_ptr<ValueVector>> vectorsToAppend;
    shared_ptr<VarLengthJoinSharedState> varLengthSharedState;
    shared_ptr<FactorizedTable> localFactorizedTable;
};

} // namespace processor
} // namespace graphflow
