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

    // void init(ExecutionContext* context, Graph* graph) {
    //     for (auto& [tableID, maxOffset] : maxOffsetMap) {
    //         denseObjects.allocate(tableID, maxOffset, context->clientContext->getMemoryManager());
    //         pinTableID(tableID);
    //         for (auto i = 0; i < maxOffset; i++) {
    //             curData[i].store(0);
    //         }
    //     }
    // }

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
    MultiplicitiesPair() : densityState{GDSDensityState::SPARSE} {
        spareObjects = GDSSpareObjectManager<multiplicity_t>();
        curSparseMultiplicities = std::make_unique<SparseMultiplicitiesReference>(spareObjects);
        nextSparseMultiplicities = std::make_unique<SparseMultiplicitiesReference>(spareObjects);
        denseObjects = GDSDenseObjectManager<std::atomic<multiplicity_t>>();
        curDenseMultiplicities = std::make_unique<DenseMultiplicitiesReference>(denseObjects);
        nextDenseMultiplicities = std::make_unique<DenseMultiplicitiesReference>(denseObjects);
    }

    void pinCurTableID(table_id_t tableID) {
        switch (densityState) {
        case GDSDensityState::SPARSE: {
            curSparseMultiplicities->pinTableID(fromTableID);
            // nextSparseMultiplicities->pinTableID(toTableID);
            curMultiplicities = curSparseMultiplicities.get();
            // nextMultiplicities = nextSparseMultiplicities.get();
        } break;
        case GDSDensityState::DENSE: {

        } break;
        default:
            KU_UNREACHABLE;
        }
    }

    void pinNextTableID(table_id_t tableID) {

    }

    void beginFrontierCompute(table_id_t fromTableID, table_id_t toTableID) {
        switch (densityState) {
        case GDSDensityState::SPARSE: {
            curSparseMultiplicities->pinTableID(fromTableID);
            nextSparseMultiplicities->pinTableID(toTableID);
            curMultiplicities = curSparseMultiplicities.get();
            nextMultiplicities = nextSparseMultiplicities.get();
        } break;
        case GDSDensityState::DENSE: {

        } break;
        default:
            KU_UNREACHABLE;
        }
    }

    void swithToDense() {
        // TODO: implement me
    }

private:
    GDSDensityState densityState;
    GDSSpareObjectManager<multiplicity_t> spareObjects;
    std::unique_ptr<SparseMultiplicitiesReference> curSparseMultiplicities;
    std::unique_ptr<SparseMultiplicitiesReference> nextSparseMultiplicities;
    GDSDenseObjectManager<std::atomic<multiplicity_t>> denseObjects;
    std::unique_ptr<DenseMultiplicitiesReference> curDenseMultiplicities;
    std::unique_ptr<DenseMultiplicitiesReference> nextDenseMultiplicities;

    Multiplicities* curMultiplicities = nullptr;
    Multiplicities* nextMultiplicities = nullptr;
};

// class DenseMultiplicities {
// public:
//     void incrementTargetMultiplicity(offset_t offset, uint64_t multiplicity) {
//         curTargetMultiplicities[offset].fetch_add(multiplicity);
//     }
//
//     uint64_t getBoundMultiplicity(offset_t nodeOffset) {
//         return curBoundMultiplicities[nodeOffset].load(std::memory_order_relaxed);
//     }
//
//     uint64_t getTargetMultiplicity(offset_t nodeOffset) {
//         return curTargetMultiplicities[nodeOffset].load(std::memory_order_relaxed);
//     }
//
//     void pinBoundTable(table_id_t tableID) {
//         curBoundMultiplicities = multiplicityArray.getData(tableID);
//     }
//
//     void pinTargetTable(table_id_t tableID) {
//         curTargetMultiplicities = multiplicityArray.getData(tableID);
//     }
//
// private:
//     ObjectArraysMap<std::atomic<uint64_t>> multiplicityArray;
//     // curTargetMultiplicities is the multiplicities of the current table that will be updated in a
//     // particular rel table extension.
//     std::atomic<uint64_t>* curTargetMultiplicities = nullptr;
//     // curBoundMultiplicities is the multiplicities of the table "from which" an extension is
//     // being made.
//     std::atomic<uint64_t>* curBoundMultiplicities = nullptr;
// };

class ASPDestinationsAuxiliaryState : public GDSAuxiliaryState {
public:
    explicit ASPDestinationsAuxiliaryState(std::shared_ptr<Multiplicities> multiplicities)
        : multiplicities{std::move(multiplicities)} {}

    void initSource(common::nodeID_t source) override {
        multiplicities->pinTargetTable(source.tableID);
        multiplicities->incrementTargetMultiplicity(source.offset, 1);
    }

    void beginFrontierCompute(common::table_id_t fromTableID,
        common::table_id_t toTableID) override {
        multiplicities->pinBoundTable(fromTableID);
        multiplicities->pinTargetTable(toTableID);
    }

private:
    std::unique_ptr<MultiplicitiesPair> multiplicitiesPair;
};

class ASPDestinationsOutputWriter : public RJOutputWriter {
public:
    ASPDestinationsOutputWriter(main::ClientContext* context, NodeOffsetMaskMap* outputNodeMask,
        nodeID_t sourceNodeID, std::shared_ptr<PathLengths> pathLengths,
        std::shared_ptr<Multiplicities> multiplicities)
        : RJOutputWriter{context, outputNodeMask, sourceNodeID},
          pathLengths{std::move(pathLengths)}, multiplicities{std::move(multiplicities)} {
        lengthVector = createVector(LogicalType::UINT16());
    }

    void beginWriting(common::table_id_t tableID) override {

    }

    void beginWritingOutputsInternal(common::table_id_t tableID) override {
        pathLengths->pinTableID(tableID);
        multiplicities->pinTargetTable(tableID);
    }

    void write(FactorizedTable& fTable, nodeID_t dstNodeID, LimitCounter* counter) override {
        auto length = pathLengths->getIteration(dstNodeID.offset);
        dstNodeIDVector->setValue<nodeID_t>(0, dstNodeID);
        lengthVector->setValue<uint16_t>(0, length);
        auto multiplicity = multiplicities->getTargetMultiplicity(dstNodeID.offset);
        for (auto i = 0u; i < multiplicity; ++i) {
            fTable.append(vectors);
        }
        if (counter != nullptr) {
            counter->increase(multiplicity);
        }
    }

    std::unique_ptr<RJOutputWriter> copy() override {
        return std::make_unique<ASPDestinationsOutputWriter>(context, outputNodeMask, sourceNodeID,
            pathLengths, multiplicities);
    }

private:
    bool skipInternal(nodeID_t dstNodeID) const override {
        return dstNodeID == sourceNodeID || pathLengths->isUnvisited(dstNodeID.offset);
    }

private:
    std::unique_ptr<ValueVector> lengthVector;
    std::shared_ptr<PathLengths> pathLengths;
    std::shared_ptr<Multiplicities> multiplicities;
};

class ASPDestinationsEdgeCompute : public SPEdgeCompute {
public:
    ASPDestinationsEdgeCompute(SPFrontierPair* frontierPair,
        std::shared_ptr<Multiplicities> multiplicities)
        : SPEdgeCompute{frontierPair}, multiplicities{std::move(multiplicities)} {};

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
                multiplicities->incrementTargetMultiplicity(nbrNodeID.offset,
                    multiplicities->getBoundMultiplicity(boundNodeID.offset));
            }
            if (nbrVal == FRONTIER_UNVISITED) {
                activeNodes.push_back(nbrNodeID);
            }
        });
        return activeNodes;
    }

    std::unique_ptr<EdgeCompute> copy() override {
        return std::make_unique<ASPDestinationsEdgeCompute>(frontierPair, multiplicities);
    }

private:
    std::shared_ptr<Multiplicities> multiplicities;
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
    RJCompState getRJCompState(ExecutionContext* context, nodeID_t sourceNodeID, const RJBindData&,
        RecursiveExtendSharedState* sharedState) override {
        auto clientContext = context->clientContext;
        auto graph = sharedState->graph.get();
        auto frontier = PathLengths::getUnvisitedFrontier(context, graph);
        auto mm = clientContext->getMemoryManager();
        auto multiplicities = std::make_shared<Multiplicities>(
            graph->getMaxOffsetMap(clientContext->getTransaction()), mm);
        auto outputWriter = std::make_unique<ASPDestinationsOutputWriter>(clientContext,
            sharedState->getOutputNodeMaskMap(), sourceNodeID, frontier, multiplicities);
        auto frontierPair = std::make_unique<SPFrontierPair>(frontier);
        auto edgeCompute =
            std::make_unique<ASPDestinationsEdgeCompute>(frontierPair.get(), multiplicities);
        auto auxiliaryState = std::make_unique<ASPDestinationsAuxiliaryState>(multiplicities);
        auto gdsState = std::make_unique<GDSComputeState>(std::move(frontierPair),
            std::move(edgeCompute), std::move(auxiliaryState));
        return RJCompState(std::move(gdsState), std::move(outputWriter));
    }
};

std::unique_ptr<RJAlgorithm> AllSPDestinationsFunction::getAlgorithm() {
    return std::make_unique<AllSPDestinationsAlgorithm>();
}

} // namespace function
} // namespace kuzu
