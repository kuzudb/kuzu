#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "binder/expression/literal_expression.h"
#include "common/types/types.h"
#include "function/gds/gds_function_collection.h"
#include "function/gds/ife_morsel.h"
#include "function/gds/parallel_utils.h"
#include "function/gds_function.h"
#include "graph/on_disk_graph.h"

using namespace kuzu::common;
using namespace kuzu::binder;

namespace kuzu {
namespace function {

struct ParallelShortestPathBindData final : public GDSBindData {
    uint8_t upperBound;

    ParallelShortestPathBindData(std::shared_ptr<Expression> nodeInput, uint8_t upperBound)
        : GDSBindData{std::move(nodeInput)}, upperBound{upperBound} {}
    ParallelShortestPathBindData(const ParallelShortestPathBindData& other)
        : GDSBindData{other}, upperBound{other.upperBound} {}

    std::unique_ptr<GDSBindData> copy() const override {
        return std::make_unique<ParallelShortestPathBindData>(*this);
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
        dstNodeIDVector->state = std::make_shared<DataChunkState>();
        lengthVector->state = dstNodeIDVector->state;
        outputVectors.push_back(srcNodeIDVector.get());
        outputVectors.push_back(dstNodeIDVector.get());
        outputVectors.push_back(lengthVector.get());
        nbrScanState = std::make_unique<graph::NbrScanState>(mm);
    }

public:
    std::unique_ptr<ValueVector> srcNodeIDVector;
    std::unique_ptr<ValueVector> dstNodeIDVector;
    std::unique_ptr<ValueVector> lengthVector;
};

class ParallelShortestPath : public GDSAlgorithm {
public:
    ParallelShortestPath() = default;
    ParallelShortestPath(const ParallelShortestPath& other) : GDSAlgorithm{other} {}

    /*
     * Inputs are
     *
     * graph::ANY
     * srcNode::NODE
     * upperBound::INT64
     */
    std::vector<common::LogicalTypeID> getParameterTypeIDs() const override {
        return {LogicalTypeID::ANY, LogicalTypeID::NODE, LogicalTypeID::INT64};
    }

    /*
     * Outputs are
     *
     * srcNode._id::INTERNAL_ID
     * dst::INTERNAL_ID
     * length::INT64
     */
    binder::expression_vector getResultColumns(binder::Binder* binder) const override {
        expression_vector columns;
        columns.push_back(bindData->nodeInput->constCast<NodeExpression>().getInternalID());
        columns.push_back(binder->createVariable("dst", *LogicalType::INTERNAL_ID()));
        columns.push_back(binder->createVariable("length", *LogicalType::INT64()));
        return columns;
    }

    void bind(const binder::expression_vector& params) override {
        KU_ASSERT(params.size() == 3);
        auto inputNode = params[1];
        ExpressionUtil::validateExpressionType(*params[2], ExpressionType::LITERAL);
        ExpressionUtil::validateDataType(*params[2], *LogicalType::INT64());
        auto upperBound = params[2]->constCast<LiteralExpression>().getValue().getValue<int64_t>();
        bindData = std::make_unique<ParallelShortestPathBindData>(inputNode, upperBound);
    }

    void initLocalState(main::ClientContext* context) override {
        localState = std::make_unique<ParallelShortestPathLocalState>(context);
    }

    static uint64_t visitNbrs(IFEMorsel* ifeMorsel, ValueVector* dstNodeIDVector) {
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
    static common::offset_t extendFrontierFunc(std::shared_ptr<GDSCallSharedState> &sharedState,
        GDSLocalState *localState) {
        auto& graph = sharedState->graph;
        auto shortestPathLocalState = common::ku_dynamic_cast<GDSLocalState*, ParallelShortestPathLocalState*>(localState);
        auto ifeMorsel = sharedState->ifeMorsel;
        auto frontierMorsel = ifeMorsel->getMorsel(64LU /* morsel size, hard-coding for now */);
        uint64_t numDstVisitedLocal = 0u;
        ValueVector* dstNodeIDVector;
        while (!ifeMorsel->isCompleteNoLock() && frontierMorsel.hasMoreToOutput()) {
            for (auto i = frontierMorsel.startOffset; i < frontierMorsel.endOffset; i++) {
                auto frontierOffset = ifeMorsel->bfsLevelNodeOffsets[i];
                graph->initializeStateFwdNbrs(frontierOffset, shortestPathLocalState->nbrScanState.get());
                do {
                    dstNodeIDVector = graph->getFwdNbrs(shortestPathLocalState->nbrScanState.get());
                    numDstVisitedLocal += visitNbrs(ifeMorsel, dstNodeIDVector);
                } while (graph->hasMoreFwdNbrs(shortestPathLocalState->nbrScanState.get()));
            }
            ifeMorsel->mergeResults(numDstVisitedLocal);
            frontierMorsel = ifeMorsel->getMorsel(64LU);
        }
        return 0;
    }

    static common::offset_t shortestPathOutputFunc(std::shared_ptr<GDSCallSharedState> &sharedState,
        GDSLocalState *localState) {
        auto ifeMorsel = sharedState->ifeMorsel;
        auto morsel = ifeMorsel->getDstWriteMorsel(DEFAULT_VECTOR_CAPACITY);
        auto shortestPathLocalState = common::ku_dynamic_cast<GDSLocalState*, ParallelShortestPathLocalState*>(localState);
        if (!morsel.hasMoreToOutput()) {
            return 0;
        }
        auto tableID = sharedState->graph->getNodeTableID();
        auto& srcNodeVector = shortestPathLocalState->srcNodeIDVector;
        auto& dstOffsetVector = shortestPathLocalState->dstNodeIDVector;
        auto& pathLengthVector = shortestPathLocalState->lengthVector;
        srcNodeVector->setValue<nodeID_t>(0, {sharedState->ifeMorsel->srcOffset, tableID});
        auto pos = 0;
        for (auto offset = morsel.startOffset; offset < morsel.endOffset; offset++) {
            auto state = ifeMorsel->visitedNodes[offset].load(std::memory_order_acq_rel);
            uint64_t pathLength = ifeMorsel->pathLength[offset].load(std::memory_order_acq_rel);
            if ((state == VISITED_DST_NEW || state == VISITED_DST) &&
                pathLength >= ifeMorsel->lowerBound) {
                dstOffsetVector->setValue<nodeID_t>(pos, {offset, tableID});
                pathLengthVector->setValue<uint64_t>(pos, pathLength);
                pos++;
            }
        }
        if (pos == 0) {
            return shortestPathOutputFunc(sharedState, localState);
        }
        dstOffsetVector->state->getSelVectorUnsafe().setSelSize(pos);
        return pos;
    }

    void exec(processor::ExecutionContext *executionContext) override {
        auto extraData = bindData->ptrCast<ParallelShortestPathBindData>();
        auto numNodes = sharedState->graph->getNumNodes();
        auto ifeMorsel = std::make_unique<IFEMorsel>(extraData->upperBound, 1 /*lower bound*/,
            numNodes + 1);
        sharedState->ifeMorsel = ifeMorsel.get();
        for (auto offset = 0u; offset < numNodes; offset++) {
            if (!sharedState->inputNodeOffsetMask->isNodeMasked(offset)) {
                continue;
            }
            ifeMorsel->initSourceNoLock(offset);
            while (!ifeMorsel->isCompleteNoLock()) {
                parallelUtils->doParallel(executionContext, extendFrontierFunc);
                ifeMorsel->initializeNextFrontierNoLock();
            }
            parallelUtils->doParallel(executionContext, shortestPathOutputFunc);
        }
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
