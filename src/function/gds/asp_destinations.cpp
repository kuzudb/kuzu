#include "binder/expression/node_expression.h"
#include "function/gds/gds_function_collection.h"
#include "function/gds/rec_joins.h"
#include "processor/execution_context.h"

using namespace kuzu::processor;
using namespace kuzu::common;
using namespace kuzu::binder;
using namespace kuzu::storage;
using namespace kuzu::graph;

namespace kuzu {
namespace function {

using multiplicity_t = uint64_t;

class Multiplicities {
public:
    virtual ~Multiplicities() = default;

    virtual void pinTableID(table_id_t tableID) = 0;

    virtual void increaseMultiplicity(offset_t offset, multiplicity_t multiplicity) = 0;

    virtual multiplicity_t getMultiplicity(offset_t offset) = 0;
};

class SparseMultiplicitiesReference : public Multiplicities {
public:
    SparseMultiplicitiesReference(GDSSpareObjectManager<multiplicity_t>& spareObjects)
        : spareObjects{spareObjects} {}

    void pinTableID(table_id_t tableID) override {
        curData = spareObjects.getData(tableID);
    }

    void increaseMultiplicity(offset_t offset, multiplicity_t multiplicity) override {
        KU_ASSERT(curData);
        if (curData->contains(offset)) {
            curData->at(offset) += multiplicity;
        } else {
            curData->insert({offset, multiplicity});
        }
    }

    multiplicity_t getMultiplicity(offset_t offset) override {
        KU_ASSERT(curData);
        if (!curData->contains(offset)) {
            return curData->at(offset);
        }
        return 0;
    }

private:
    GDSSpareObjectManager<multiplicity_t>& spareObjects;
    std::unordered_map<offset_t, multiplicity_t>* curData = nullptr;
};

class DenseMultiplicitiesReference : public Multiplicities {
public:
    DenseMultiplicitiesReference(GDSDenseObjectManager<std::atomic<multiplicity_t>>& denseObjects)
        : denseObjects(denseObjects) {}

    void pinTableID(table_id_t tableID) override {
        curData = denseObjects.getData(tableID);
    }

    void increaseMultiplicity(offset_t offset, multiplicity_t multiplicity) override {
        KU_ASSERT(curData);
        curData[offset].fetch_add(multiplicity);
    }

    multiplicity_t getMultiplicity(offset_t offset) override {
        KU_ASSERT(curData);
        return curData[offset].load(std::memory_order_relaxed);
    }

private:
    GDSDenseObjectManager<std::atomic<multiplicity_t>>& denseObjects;
    std::atomic<multiplicity_t>* curData = nullptr;
};

class MultiplicitiesPair {
public:
    MultiplicitiesPair(table_id_map_t<offset_t> maxOffsetMap) : maxOffsetMap{maxOffsetMap},
        densityState{GDSDensityState::SPARSE} {
        sparseObjects = GDSSpareObjectManager<multiplicity_t>();
        curSparseMultiplicities = std::make_unique<SparseMultiplicitiesReference>(sparseObjects);
        nextSparseMultiplicities = std::make_unique<SparseMultiplicitiesReference>(sparseObjects);
        denseObjects = GDSDenseObjectManager<std::atomic<multiplicity_t>>();
        curDenseMultiplicities = std::make_unique<DenseMultiplicitiesReference>(denseObjects);
        nextDenseMultiplicities = std::make_unique<DenseMultiplicitiesReference>(denseObjects);
    }

    void pinCurTableID(table_id_t tableID) {
        switch (densityState) {
        case GDSDensityState::SPARSE: {
            curSparseMultiplicities->pinTableID(tableID);
            curMultiplicities = curSparseMultiplicities.get();
        } break;
        case GDSDensityState::DENSE: {
            curDenseMultiplicities->pinTableID(tableID);
            curMultiplicities = curDenseMultiplicities.get();
        } break;
        default:
            KU_UNREACHABLE;
        }
    }

    void pinNextTableID(table_id_t tableID) {
        switch (densityState) {
        case GDSDensityState::SPARSE: {
            nextSparseMultiplicities->pinTableID(tableID);
            nextMultiplicities = nextSparseMultiplicities.get();
        } break;
        case GDSDensityState::DENSE: {

        } break;
        default:
            KU_UNREACHABLE;
        }
    }

    void increaseNextMultiplicity(offset_t offset, multiplicity_t multiplicity) {
        nextMultiplicities->increaseMultiplicity(offset, multiplicity);
    }

    multiplicity_t getCurrentMultiplicity(offset_t offset) {
        return curMultiplicities->getMultiplicity(offset);
    }
    Multiplicities* getCurrentMultiplicities() {
        return curMultiplicities;
    }

    void switchToDense(ExecutionContext* context) {
        KU_ASSERT(densityState == GDSDensityState::SPARSE);
        densityState = GDSDensityState::DENSE;
        for (auto& [tableID, maxOffset] : maxOffsetMap) {
            denseObjects.allocate(tableID, maxOffset, context->clientContext->getMemoryManager());
            auto data = denseObjects.getData(tableID);
            for (auto i = 0; i < maxOffset; i++) {
                data[i].store(0);
            }
        }
        for (auto& [tableID, map] : sparseObjects) {
            auto data = denseObjects.getData(tableID);
            for (auto [offset, multiplicity] : map) {
                data[offset].store(multiplicity);
            }
        }
    }

private:
    table_id_map_t<offset_t> maxOffsetMap;
    GDSDensityState densityState;
    GDSSpareObjectManager<multiplicity_t> sparseObjects;
    std::unique_ptr<SparseMultiplicitiesReference> curSparseMultiplicities;
    std::unique_ptr<SparseMultiplicitiesReference> nextSparseMultiplicities;
    GDSDenseObjectManager<std::atomic<multiplicity_t>> denseObjects;
    std::unique_ptr<DenseMultiplicitiesReference> curDenseMultiplicities;
    std::unique_ptr<DenseMultiplicitiesReference> nextDenseMultiplicities;

    Multiplicities* curMultiplicities = nullptr;
    Multiplicities* nextMultiplicities = nullptr;
};

class ASPDestinationsAuxiliaryState : public GDSAuxiliaryState {
public:
    explicit ASPDestinationsAuxiliaryState(std::unique_ptr<MultiplicitiesPair> multiplicitiesPair)
        : multiplicitiesPair{std::move(multiplicitiesPair)} {}

    MultiplicitiesPair* getMultiplicitiesPair() { return multiplicitiesPair.get(); }

    void initSource(nodeID_t source) override {
        multiplicitiesPair->pinNextTableID(source.tableID);
        multiplicitiesPair->increaseNextMultiplicity(source.offset, 1);
    }

    void beginFrontierCompute(table_id_t curTableID, table_id_t nextTableID) override {
        multiplicitiesPair->pinCurTableID(curTableID);
        multiplicitiesPair->pinNextTableID(nextTableID);
    }

    void switchToDense(ExecutionContext* context, Graph* graph) override {
        multiplicitiesPair->switchToDense(context);
    }

private:
    std::unique_ptr<MultiplicitiesPair> multiplicitiesPair;
};

class ASPDestinationsOutputWriter : public RJOutputWriter {
public:
    ASPDestinationsOutputWriter(main::ClientContext* context, NodeOffsetMaskMap* outputNodeMask,
        nodeID_t sourceNodeID, Frontier* frontier, Multiplicities* multiplicities)
        : RJOutputWriter{context, outputNodeMask, sourceNodeID}, frontier{frontier}, multiplicities{multiplicities} {
        lengthVector = createVector(LogicalType::UINT16());
    }

    void beginWriting(table_id_t tableID) override {
        frontier->pinTableID(tableID);
        multiplicities->pinTableID(tableID);
    }

    void write(FactorizedTable& fTable, nodeID_t dstNodeID, LimitCounter* counter) override {
        auto iter = frontier->getIteration(dstNodeID.offset);
        if (iter == FRONTIER_UNVISITED) {
            return;
        }
        dstNodeIDVector->setValue<nodeID_t>(0, dstNodeID);
        lengthVector->setValue<uint16_t>(0, iter);
        auto multiplicity = multiplicities->getMultiplicity(dstNodeID.offset);
        for (auto i = 0u; i < multiplicity; ++i) {
            fTable.append(vectors);
        }
        if (counter != nullptr) {
            counter->increase(multiplicity);
        }
    }

    std::unique_ptr<RJOutputWriter> copy() override {
        return std::make_unique<ASPDestinationsOutputWriter>(context, outputNodeMask, sourceNodeID,
            frontier, multiplicities);
    }

private:
    std::unique_ptr<ValueVector> lengthVector;
    Frontier* frontier;
    Multiplicities* multiplicities;
};

class ASPDestinationsEdgeCompute : public SPEdgeCompute {
public:
    ASPDestinationsEdgeCompute(SPFrontierPair* frontierPair, MultiplicitiesPair* multiplicitiesPair)
        : SPEdgeCompute{frontierPair}, multiplicitiesPair{multiplicitiesPair} {};

    std::vector<nodeID_t> edgeCompute(nodeID_t boundNodeID, NbrScanState::Chunk& resultChunk,
        bool) override {
        std::vector<nodeID_t> activeNodes;
        resultChunk.forEach([&](auto nbrNodeID, auto /*edgeID*/) {
            auto nbrVal = frontierPair->getNextFrontierValue(nbrNodeID.offset);
            // We should update the nbrID's multiplicity in 2 cases: 1) if nbrID is being visited
            // for the first time, i.e., when its value in the pathLengths frontier is
            // FRONTIER_UNVISITED. Or 2) if nbrID has already been visited but in this
            // iteration, so it's value is curIter + 1.
            auto shouldUpdate =
                nbrVal == FRONTIER_UNVISITED || nbrVal == frontierPair->getCurrentIter();
            if (shouldUpdate) {
                // Note: This is safe because curNodeID is in the current frontier, so its
                // shortest paths multiplicity is guaranteed to not change in the current iteration.
                auto boundMultiplicity = multiplicitiesPair->getCurrentMultiplicity(boundNodeID.offset);
                multiplicitiesPair->increaseNextMultiplicity(nbrNodeID.offset, boundMultiplicity);
            }
            if (nbrVal == FRONTIER_UNVISITED) {
                activeNodes.push_back(nbrNodeID);
            }
        });
        return activeNodes;
    }

    std::unique_ptr<EdgeCompute> copy() override {
        return std::make_unique<ASPDestinationsEdgeCompute>(frontierPair, multiplicitiesPair);
    }

private:
    MultiplicitiesPair* multiplicitiesPair;
};

/**
 * Algorithm for parallel all shortest paths computation, so all shortest paths from a source to
 * is returned for each destination. If paths are not returned, multiplicities indicate the
 * number of paths to each destination.
 */
class AllSPDestinationsAlgorithm final : public RJAlgorithm {
public:
    std::string getFunctionName() const override { return AllSPDestinationsFunction::name; }

    expression_vector getResultColumns(const RJBindData& bindData) const override {
        expression_vector columns;
        columns.push_back(bindData.nodeInput->constCast<NodeExpression>().getInternalID());
        columns.push_back(bindData.nodeOutput->constCast<NodeExpression>().getInternalID());
        columns.push_back(bindData.lengthExpr);
        return columns;
    }

    std::unique_ptr<RJAlgorithm> copy() const override {
        return std::make_unique<AllSPDestinationsAlgorithm>(*this);
    }

private:
    std::unique_ptr<GDSComputeState> getComputeState(ExecutionContext* context, const RJBindData& bindData, RecursiveExtendSharedState* sharedState) override {
        auto clientContext = context->clientContext;
        auto graph = sharedState->graph.get();
        auto multiplicitiesPair = std::make_unique<MultiplicitiesPair>(graph->getMaxOffsetMap(clientContext->getTransaction()));
        auto frontier = DenseFrontier::getUnvisitedFrontier(context, graph);
        auto frontierPair = std::make_unique<SPFrontierPair>(frontier);
        auto edgeCompute =
            std::make_unique<ASPDestinationsEdgeCompute>(frontierPair.get(), multiplicitiesPair.get());
        auto auxiliaryState = std::make_unique<ASPDestinationsAuxiliaryState>(std::move(multiplicitiesPair));
        return std::make_unique<GDSComputeState>(std::move(frontierPair),
            std::move(edgeCompute), std::move(auxiliaryState));
    }

    std::unique_ptr<RJOutputWriter> getOutputWriter(ExecutionContext* context, const RJBindData& bindData, GDSComputeState& computeState, nodeID_t sourceNodeID, RecursiveExtendSharedState* sharedState) override {
        auto clientContext = context->clientContext;
        Frontier* frontier = nullptr;
        switch (computeState.frontierPair->getState()) {
        case GDSDensityState::DENSE: {
            frontier = &computeState.frontierPair->ptrCast<SPFrontierPair>()->getDenseFrontier();
        } break;
        case GDSDensityState::SPARSE: {
            frontier = &computeState.visitedSparseFrontier;
        } break;
        default:
            KU_UNREACHABLE;
        }
        auto multiplicities = computeState.auxiliaryState->ptrCast<ASPDestinationsAuxiliaryState>()->getMultiplicitiesPair()->getCurrentMultiplicities();
        return std::make_unique<ASPDestinationsOutputWriter>(clientContext,
            sharedState->getOutputNodeMaskMap(), sourceNodeID, frontier, multiplicities);
    }
};

std::unique_ptr<RJAlgorithm> AllSPDestinationsFunction::getAlgorithm() {
    return std::make_unique<AllSPDestinationsAlgorithm>();
}

} // namespace function
} // namespace kuzu
