#pragma once

#include "function/algorithm/parallel_utils.h"
#include "processor/operator/sink.h"
#include "processor/operator/call/in_query_call.h"

namespace kuzu {
namespace processor {

class AlgorithmRunner : public PhysicalOperator {
public:
    AlgorithmRunner(InQueryCallInfo info, std::shared_ptr<InQueryCallSharedState> sharedState,
        std::shared_ptr<graph::GraphAlgorithm> graphAlgorithm,
        std::shared_ptr<graph::ParallelUtils> parallelUtils, uint32_t id,
        const std::string& paramString) : PhysicalOperator{PhysicalOperatorType::ALGORITHM_RUNNER,
              id, paramString}, info{std::move(info)}, sharedState{std::move(sharedState)},
          graphAlgorithm{std::move(graphAlgorithm)}, parallelUtils{std::move(parallelUtils)} {}

    bool isSource() const override { return true; }

    void initGlobalStateInternal(ExecutionContext* context) final;

    void initLocalStateInternal(ResultSet* resultSet_, ExecutionContext* context) final;

    bool getNextTuplesInternal(ExecutionContext* context) final;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<AlgorithmRunner>(info.copy(), sharedState, graphAlgorithm,
            parallelUtils, id, paramsString);
    }

private:
    bool isMaster;
    InQueryCallInfo info;
    std::shared_ptr<InQueryCallSharedState> sharedState;
    InQueryCallLocalState localState;
    std::shared_ptr<graph::GraphAlgorithm> graphAlgorithm;
    std::shared_ptr<graph::ParallelUtils> parallelUtils;
};

}
}
