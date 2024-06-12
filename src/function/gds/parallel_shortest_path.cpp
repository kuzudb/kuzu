#include "function/gds/ife_morsel.h"
#include "common/types/types.h"
#include "function/gds/gds_function_collection.h"
#include "function/gds/parallel_utils.h"
#include "function/gds_function.h"
#include "binder/expression/expression_util.h"
#include "binder/expression/literal_expression.h"

using namespace kuzu::common;
using namespace kuzu::binder;

namespace kuzu {
namespace function {

struct ParallelShortestPathBindData final : public GDSBindData {
    uint8_t upperBound;

    explicit ParallelShortestPathBindData(uint8_t upperBound) : upperBound{upperBound} {}

    std::unique_ptr<GDSBindData> copy() const override {
        return std::make_unique<ParallelShortestPathBindData>(upperBound);
    }
};

class ParallelShortestPathLocalState : public GDSLocalState {
public:
    explicit ParallelShortestPathLocalState(main::ClientContext *clientContext) {
        auto mm = clientContext->getMemoryManager();
        srcNodeIDVector = std::make_unique<ValueVector>(*LogicalType::INTERNAL_ID(), mm);
        dstNodeIDVector = std::make_unique<ValueVector>(*LogicalType::INTERNAL_ID(), mm);
        lengthVector = std::make_unique<ValueVector>(*LogicalType::INT64(), mm);
        srcNodeIDVector->state = DataChunkState::getSingleValueDataChunkState();
        dstNodeIDVector->state = DataChunkState::getSingleValueDataChunkState();
        lengthVector->state = DataChunkState::getSingleValueDataChunkState();
        vectors.push_back(srcNodeIDVector.get());
        vectors.push_back(dstNodeIDVector.get());
        vectors.push_back(lengthVector.get());
    }

private:
    std::unique_ptr<ValueVector> srcNodeIDVector;
    std::unique_ptr<ValueVector> dstNodeIDVector;
    std::unique_ptr<ValueVector> lengthVector;
    std::vector<ValueVector*> vectors;
};

class ParallelShortestPath : public GDSAlgorithm {
public:
    ParallelShortestPath() = default;
    ParallelShortestPath(const ParallelShortestPath& other) : GDSAlgorithm{other} {}

    std::vector<common::LogicalTypeID> getParameterTypeIDs() const override {
        return {LogicalTypeID::ANY, LogicalTypeID::INT64};
    }

    std::vector<std::string> getResultColumnNames() const override {
        return {"src", "dst", "length"};
    }

    std::vector<common::LogicalType> getResultColumnTypes() const override {
        return {*LogicalType::INTERNAL_ID(), *LogicalType::INTERNAL_ID(), *LogicalType::INT64()};
    }

    void bind(const binder::expression_vector& params) override {
        ExpressionUtil::validateExpressionType(*params[1], ExpressionType::LITERAL);
        auto upperBound = params[1]->constCast<LiteralExpression>().getValue().getValue<int64_t>();
        bindData = std::make_unique<ParallelShortestPathBindData>(upperBound);
    }

    void initLocalState(main::ClientContext* context) override {
        localState = std::make_unique<ParallelShortestPathLocalState>(context);
    }

    uint64_t visitNbrs(IFEMorsel* ifeMorsel, ValueVector* dstNodeIDVector) {
        uint64_t numDstVisitedLocal = 0u;
        for (auto j = 0u; j < dstNodeIDVector->state->getSelVector().getSelSize(); j++) {
            auto pos = dstNodeIDVector->state->getSelVector().operator[](j);
            auto dstNodeID = dstNodeIDVector->getValue<common::nodeID_t>(pos);
            auto state = ifeMorsel->visitedNodes[dstNodeID.offset].load(std::memory_order_acq_rel);
            if (state == NOT_VISITED_DST) {
                auto tryCAS = ifeMorsel->visitedNodes[dstNodeID.offset].compare_exchange_strong(state,
                    VISITED_DST_NEW, std::memory_order_acq_rel);
                if (tryCAS) {
                    numDstVisitedLocal++;
                    ifeMorsel->pathLength[dstNodeID.offset].store(ifeMorsel->currentLevel + 1,
                        std::memory_order_relaxed);
                }
            } else if (state == NOT_VISITED) {
                ifeMorsel->visitedNodes[dstNodeID.offset].store(VISITED_NEW, std::memory_order_acq_rel);
            }
        }
        return numDstVisitedLocal;
    }

    /*
     * This will be main function for logic, doing frontier extension and updating state.
     */
    common::offset_t extendFrontierFunc(TableFuncInput& input, TableFuncOutput& /*output*/) {
        auto sharedState =
            ku_dynamic_cast<TableFuncSharedState*, ShortestPathAlgoSharedState*>(input.sharedState);
        auto localState =
            ku_dynamic_cast<TableFuncLocalState*, ShortestPathAlgoLocalState*>(input.localState);
        auto& graph = sharedState->graph;
        auto& ifeMorsel = sharedState->ifeMorsel;
        auto frontierMorsel = ifeMorsel->getMorsel(sharedState->morselSize);
        uint64_t numDstVisitedLocal = 0u;
        ValueVector* dstNodeIDVector;
        while (!ifeMorsel->isCompleteNoLock() && frontierMorsel.hasMoreToOutput()) {
            for (auto i = frontierMorsel.startOffset; i < frontierMorsel.endOffset; i++) {
                auto frontierOffset = ifeMorsel->bfsLevelNodeOffsets[i];
                graph->initializeStateFwdNbrs(frontierOffset, localState->nbrScanState.get());
                do {
                    dstNodeIDVector = graph->getFwdNbrs(localState->nbrScanState.get());
                    numDstVisitedLocal += visitNbrs(ifeMorsel.get(), dstNodeIDVector);
                } while (graph->hasMoreFwdNbrs(localState->nbrScanState.get()));
            }
            ifeMorsel->mergeResults(numDstVisitedLocal);
            frontierMorsel = ifeMorsel->getMorsel(sharedState->morselSize);
        }
        return 0;
    }

    common::offset_t shortestPathOutputFunc(TableFuncInput& input, TableFuncOutput& output) {
        auto sharedState =
            ku_dynamic_cast<TableFuncSharedState*, ShortestPathAlgoSharedState*>(input.sharedState);
        auto& ifeMorsel = sharedState->ifeMorsel;
        auto morselSize = DEFAULT_VECTOR_CAPACITY;
        auto morsel = ifeMorsel->getDstWriteMorsel(morselSize);
        if (!morsel.hasMoreToOutput()) {
            return 0;
        }
        auto dstOffsetVector = output.dataChunk.valueVectors[0];
        auto pathLengthVector = output.dataChunk.valueVectors[1];
        auto pos = 0;
        for (auto offset = morsel.startOffset; offset < morsel.endOffset; offset++) {
            auto state = ifeMorsel->visitedNodes[offset].load(std::memory_order_acq_rel);
            uint64_t pathLength = ifeMorsel->pathLength[offset].load(std::memory_order_acq_rel);
            if ((state == VISITED_DST_NEW || state == VISITED_DST) &&
                pathLength >= ifeMorsel->lowerBound) {
                dstOffsetVector->setValue<uint64_t>(pos, offset);
                pathLengthVector->setValue<uint64_t>(pos, pathLength);
                pos++;
            }
        }
        if (pos == 0) {
            return shortestPathOutputFunc(input, output);
        }
        dstOffsetVector->state->getSelVectorUnsafe().setSelSize(pos);
        return pos;
    }

    void exec(processor::ExecutionContext *executionContext) override {
        auto gdsCallSharedState = parallelUtils->getGDSCallSharedState();
        auto algoSharedState = ku_dynamic_cast<GDSCallSharedState*, ShortestPathAlgoSharedState*>(
            gdsCallSharedState);
        auto& ifeMorsel = algoSharedState->ifeMorsel;
        ifeMorsel->initSourceNoLock(algoSharedState->srcOffset);
        while (!ifeMorsel->isCompleteNoLock()) {
            parallelUtils->doParallel(executionContext, extendFrontierFunc);
            ifeMorsel->initializeNextFrontierNoLock();
        }
        parallelUtils->doParallel(executionContext, shortestPathOutputFunc);
    }

    std::unique_ptr<GDSAlgorithm> copy() const override {
        return std::make_unique<ParallelShortestPath>(*this);
    }
};

function_set ParallelShortestPathsFunction::getFunctionSet() {
    function_set result;
    auto function = std::make_unique<GDSFunction>(name, std::make_unique<ParallelShortestPath>());
    result.push_back(std::move(function));
    return result;
}

}
}
