#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "binder/expression/literal_expression.h"
#include "common/exception/binder.h"
#include "function/gds/frontier.h"
#include "function/gds/gds_function_collection.h"
#include "function/gds_function.h"
#include "graph/graph.h"
#include "main/client_context.h"
#include "processor/operator/gds_call.h"
#include "processor/result/factorized_table.h"

using namespace kuzu::processor;
using namespace kuzu::common;
using namespace kuzu::binder;
using namespace kuzu::storage;

namespace kuzu {
namespace function {

struct VariableLengthPathBindData final : public GDSBindData {
    std::shared_ptr<Expression> nodeInput;
    uint8_t lowerBound;
    uint8_t upperBound;

    VariableLengthPathBindData(std::shared_ptr<Expression> nodeInput, uint8_t lowerBound,
        uint8_t upperBound)
        : nodeInput{std::move(nodeInput)}, lowerBound{lowerBound}, upperBound{upperBound} {}

    VariableLengthPathBindData(const VariableLengthPathBindData& other)
        : nodeInput{other.nodeInput}, lowerBound{other.lowerBound}, upperBound{other.upperBound} {}

    bool hasNodeInput() const override { return true; }
    std::shared_ptr<binder::Expression> getNodeInput() const override { return nodeInput; }

    std::unique_ptr<GDSBindData> copy() const override {
        return std::make_unique<VariableLengthPathBindData>(*this);
    }
};

struct VariableLengthPathSourceState {
    nodeID_t sourceNodeID;
    Frontier currentFrontier;
    Frontier nextFrontier;

    explicit VariableLengthPathSourceState(nodeID_t sourceNodeID) : sourceNodeID{sourceNodeID} {
        currentFrontier = Frontier();
        currentFrontier.addNode(sourceNodeID, 1 /* multiplicity */);
        nextFrontier = Frontier();
    }

    void initNextFrontier() {
        currentFrontier = std::move(nextFrontier);
        nextFrontier = Frontier();
    }
};

class VariableLengthPathLocalState : public GDSLocalState {
public:
    explicit VariableLengthPathLocalState(main::ClientContext* context) {
        auto mm = context->getMemoryManager();
        srcNodeIDVector = std::make_unique<ValueVector>(*LogicalType::INTERNAL_ID(), mm);
        dstNodeIDVector = std::make_unique<ValueVector>(*LogicalType::INTERNAL_ID(), mm);
        lengthVector = std::make_unique<ValueVector>(*LogicalType::INT64(), mm);
        numPathVector = std::make_unique<ValueVector>(*LogicalType::INT64(), mm);
        srcNodeIDVector->state = DataChunkState::getSingleValueDataChunkState();
        dstNodeIDVector->state = DataChunkState::getSingleValueDataChunkState();
        lengthVector->state = DataChunkState::getSingleValueDataChunkState();
        numPathVector->state = DataChunkState::getSingleValueDataChunkState();
        vectors.push_back(srcNodeIDVector.get());
        vectors.push_back(dstNodeIDVector.get());
        vectors.push_back(lengthVector.get());
        vectors.push_back(numPathVector.get());
    }

    void materialize(const VariableLengthPathSourceState& sourceState, uint8_t length,
        FactorizedTable& table) const {
        srcNodeIDVector->setValue<nodeID_t>(0, sourceState.sourceNodeID);
        for (auto dstNodeID : sourceState.nextFrontier.getNodeIDs()) {
            dstNodeIDVector->setValue<nodeID_t>(0, dstNodeID);
            auto numPath = sourceState.nextFrontier.getMultiplicity(dstNodeID);
            lengthVector->setValue<int64_t>(0, length);
            numPathVector->setValue<int64_t>(0, numPath);
            table.append(vectors);
        }
    }

private:
    std::unique_ptr<ValueVector> srcNodeIDVector;
    std::unique_ptr<ValueVector> dstNodeIDVector;
    std::unique_ptr<ValueVector> lengthVector;
    std::unique_ptr<ValueVector> numPathVector;
    std::vector<ValueVector*> vectors;
};

class VariableLengthPaths final : public GDSAlgorithm {
public:
    VariableLengthPaths() = default;
    VariableLengthPaths(const VariableLengthPaths& other) : GDSAlgorithm{other} {}

    /*
     * Inputs are
     *
     * graph::ANY
     * srcNode::NODE
     * lowerBound::INT64
     * upperBound::INT64
     */
    std::vector<common::LogicalTypeID> getParameterTypeIDs() const override {
        return {LogicalTypeID::ANY, LogicalTypeID::NODE, LogicalTypeID::INT64,
            LogicalTypeID::INT64};
    }

    /*
     * Outputs are
     *
     * srcNode._id::INTERNAL_ID
     * dst::INTERNAL_ID
     * length::INT64
     * num_path::INT64
     */
    binder::expression_vector getResultColumns(binder::Binder* binder) const override {
        expression_vector columns;
        auto& inputNode = bindData->getNodeInput()->constCast<NodeExpression>();
        columns.push_back(inputNode.getInternalID());
        columns.push_back(binder->createVariable("dst", *LogicalType::INTERNAL_ID()));
        columns.push_back(binder->createVariable("length", *LogicalType::INT64()));
        columns.push_back(binder->createVariable("num_path", *LogicalType::INT64()));
        return columns;
    }

    void bind(const binder::expression_vector& params) override {
        KU_ASSERT(params.size() == 4);
        for (auto i = 2u; i < 4u; ++i) {
            ExpressionUtil::validateExpressionType(*params[i], ExpressionType::LITERAL);
            ExpressionUtil::validateDataType(*params[i], *LogicalType::INT64());
        }
        auto lowerBound = params[2]->constCast<LiteralExpression>().getValue().getValue<int64_t>();
        if (lowerBound <= 0) {
            throw BinderException("Lower bound must be greater than 0.");
        }
        auto upperBound = params[3]->constCast<LiteralExpression>().getValue().getValue<int64_t>();
        bindData = std::make_unique<VariableLengthPathBindData>(params[1], lowerBound, upperBound);
    }

    void initLocalState(main::ClientContext* context) override {
        localState = std::make_unique<VariableLengthPathLocalState>(context);
    }

    void exec() override {
        auto extraData = bindData->ptrCast<VariableLengthPathBindData>();
        auto variableLengthPathLocalState = localState->ptrCast<VariableLengthPathLocalState>();
        auto graph = sharedState->graph.get();
        // We check every node in projected graph against semi mask.
        // Alternatively we could check semi mask directly but in such case, we need to validate
        // if a node in semi mask exist in projected graph or not.
        for (auto tableID : graph->getNodeTableIDs()) {
            if (!sharedState->inputNodeOffsetMasks.contains(tableID)) {
                continue;
            }
            auto mask = sharedState->inputNodeOffsetMasks.at(tableID).get();
            for (auto offset = 0u; offset < graph->getNumNodes(tableID); ++offset) {
                if (!mask->isNodeMasked(offset)) {
                    continue;
                }
                auto sourceNodeID = nodeID_t{offset, tableID};
                auto sourceState = VariableLengthPathSourceState(sourceNodeID);
                // Start recursive computation for current source node ID.
                for (auto currentLevel = 1; currentLevel <= extraData->upperBound; ++currentLevel) {
                    // Compute next frontier.
                    for (auto currentNodeID : sourceState.currentFrontier.getNodeIDs()) {
                        auto currentMultiplicity =
                            sourceState.currentFrontier.getMultiplicity(currentNodeID);
                        auto nbrs = graph->scanFwd(currentNodeID);
                        for (auto nbr : nbrs) {
                            sourceState.nextFrontier.addNode(nbr, currentMultiplicity);
                        }
                    }
                    if (currentLevel >= extraData->lowerBound) {
                        variableLengthPathLocalState->materialize(sourceState, currentLevel,
                            *sharedState->fTable);
                    }
                    sourceState.initNextFrontier();
                };
            }
        }
    }

    std::unique_ptr<GDSAlgorithm> copy() const override {
        return std::make_unique<VariableLengthPaths>(*this);
    }
};

function_set VariableLengthPathsFunction::getFunctionSet() {
    function_set result;
    auto function = std::make_unique<GDSFunction>(name, std::make_unique<VariableLengthPaths>());
    result.push_back(std::move(function));
    return result;
}

} // namespace function
} // namespace kuzu
