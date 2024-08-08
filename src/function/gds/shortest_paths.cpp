#include <atomic>

#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "common/exception/runtime.h"
#include "function/gds/gds_frontier.h"
#include "function/gds/gds_function_collection.h"
#include "function/gds/gds_utils.h"
#include "function/gds_function.h"
#include "processor/operator/gds_call.h"
#include "processor/result/factorized_table.h"

using namespace kuzu::processor;
using namespace kuzu::common;
using namespace kuzu::binder;
using namespace kuzu::storage;
using namespace kuzu::graph;

namespace kuzu {
namespace function {

enum class RecJoinOutputType : uint8_t {
    DESTINATION_NODES = 0,
    LENGTHS = 1,
    // PATHS returns only the intermediate nodeIDs in a path, not the source or dst nodeIDs.
    PATHS = 2,
};

/**
 * A data structure to keep track of a single (shortest) path from one source node to
 * multiple destination (and intermediate) nodes as "backward BFS graph" form. The data structure is
 * "dense" in the sense that it keeps space for one "backwards edge" for each possible node that can
 * be in the destination. Supports multi-label nodes.
 */
class SinglePaths {
public:
    explicit SinglePaths(
        std::vector<std::tuple<common::table_id_t, uint64_t>> nodeTableIDAndNumNodes,
        MemoryManager* mm) {
        for (const auto& [tableID, numNodes] : nodeTableIDAndNumNodes) {
            lastEdges.insert({tableID, mm->allocateBuffer(false, numNodes * sizeof(nodeID_t))});
        }
    }

    void setLastEdge(nodeID_t nodeID, nodeID_t lastEdge) {
        KU_ASSERT(currentFixedLastEdges != nullptr);
        currentFixedLastEdges[nodeID.offset] = lastEdge;
    }

    nodeID_t getParent(nodeID_t nodeID) {
        return reinterpret_cast<nodeID_t*>(
            lastEdges.at(nodeID.tableID).get()->buffer.data())[nodeID.offset];
    }

    void fixNodeTable(common::table_id_t tableID) {
        KU_ASSERT(lastEdges.contains(tableID));
        currentFixedLastEdges =
            reinterpret_cast<nodeID_t*>(lastEdges.at(tableID).get()->buffer.data());
    }

private:
    common::table_id_map_t<std::unique_ptr<MemoryBuffer>> lastEdges;
    nodeID_t* currentFixedLastEdges;
};

/**
 * A GDSFrontier implementation that keeps the lengths of the shortest paths from the source node to
 * destination nodes. This is a light-weight implementation that can keep lengths up to and
 * including 254. The length stored for the source node is 0. Length 255 is reserved for marking
 * nodes that are not visited. This class is intended to represent both the current and next
 * frontiers. At iteration i of the shortest path algorithm (iterations start from 0), nodes
 * with mask values i are in the current frontier. Nodes with any other values are not in the
 * frontier. Similarly at iteration i setting a node u active sets its mask value to i+1.
 * Therefore this class needs to be informed about the current iteration of the algorithm to
 * provide correct implementations of isActive and setActive functions.
 */
class PathLengths : public GDSFrontier {
    friend class PathLengthsFrontiers;

public:
    static constexpr uint8_t UNVISITED = 255;

    explicit PathLengths(
        std::vector<std::tuple<common::table_id_t, uint64_t>> nodeTableIDAndNumNodes) {
        for (const auto& [tableID, numNodes] : nodeTableIDAndNumNodes) {
            masks.insert({tableID, std::make_unique<MaskData>(numNodes, UNVISITED)});
        }
    }

    uint8_t getMaskValueFromCurFrontierFixedMask(common::offset_t nodeOffset) {
        KU_ASSERT(curFrontierFixedMask != nullptr);
        return curFrontierFixedMask->getMaskValue(nodeOffset);
    }

    uint8_t getMaskValueFromNextFrontierFixedMask(common::offset_t nodeOffset) {
        KU_ASSERT(nextFrontierFixedMask != nullptr);
        return nextFrontierFixedMask->getMaskValue(nodeOffset);
    }

    bool isActive(nodeID_t nodeID) override {
        KU_ASSERT(curFrontierFixedMask != nullptr);
        return curFrontierFixedMask->getMaskValue(nodeID.offset) == curIter;
    }

    void setActive(nodeID_t nodeID) override {
        KU_ASSERT(nextFrontierFixedMask != nullptr);
        if (nextFrontierFixedMask->isMasked(nodeID.offset, UNVISITED)) {
            // Note that if curIter = 255, this will set the mask value to 0. Therefore when
            // the next (and first) iteration of the algorithm starts, the node will be "active".
            nextFrontierFixedMask->setMask(nodeID.offset, curIter + 1);
        }
    }

    void incrementCurIter() { curIter++; }

    void fixCurFrontierNodeTable(common::table_id_t tableID) {
        KU_ASSERT(masks.contains(tableID));
        curFrontierFixedTableID = tableID;
        curFrontierFixedMask = masks.at(tableID).get();
    }

    void fixNextFrontierNodeTable(common::table_id_t tableID) {
        KU_ASSERT(masks.contains(tableID));
        nextFrontierFixedMask = masks.at(tableID).get();
    }

    uint64_t getNumNodesInCurFrontierFixedNodeTable() {
        KU_ASSERT(curFrontierFixedMask != nullptr);
        return curFrontierFixedMask->getSize();
    }

private:
    uint8_t curIter = 255;
    common::table_id_map_t<std::unique_ptr<MaskData>> masks;
    common::table_id_t curFrontierFixedTableID;
    MaskData* curFrontierFixedMask;
    MaskData* nextFrontierFixedMask;
};

class PathLengthsFrontiers : public Frontiers {
    friend struct SPLengthsFrontierCompute;
    friend struct SPSinglePathsFrontierCompute;
    static constexpr uint64_t FRONTIER_MORSEL_SIZE = 64;

public:
    explicit PathLengthsFrontiers(PathLengths* pathLengths)
        : Frontiers(pathLengths /* curFrontier */, pathLengths /* nextFrontier */,
              1 /* initial num active nodes */),
          pathLengths{pathLengths} {}

    bool getNextFrontierMorsel(RangeFrontierMorsel& frontierMorsel) override {
        if (nextOffset.load() >= pathLengths->getNumNodesInCurFrontierFixedNodeTable()) {
            return false;
        }
        auto numNodes = pathLengths->getNumNodesInCurFrontierFixedNodeTable();
        auto beginOffset = nextOffset.fetch_add(FRONTIER_MORSEL_SIZE);
        if (beginOffset >= pathLengths->getNumNodesInCurFrontierFixedNodeTable()) {
            return false;
        }
        auto endOffset = beginOffset + FRONTIER_MORSEL_SIZE > numNodes ?
                             numNodes :
                             beginOffset + FRONTIER_MORSEL_SIZE;
        frontierMorsel.initMorsel(pathLengths->curFrontierFixedTableID, beginOffset, endOffset);
        return true;
    }

    void beginFrontierComputeBetweenTables(table_id_t curFrontierTableID,
        table_id_t nextFrontierTableID) override {
        pathLengths->fixCurFrontierNodeTable(curFrontierTableID);
        pathLengths->fixNextFrontierNodeTable(nextFrontierTableID);
        nextOffset.store(0u);
    }

    void beginNewIterationInternalNoLock() override {
        nextOffset.store(0u);
        pathLengths->incrementCurIter();
    }

private:
    PathLengths* pathLengths;
    std::atomic<offset_t> nextOffset;
};

class SinglePathsFrontiers : public PathLengthsFrontiers {
    friend struct SPSinglePathsFrontierCompute;

public:
    explicit SinglePathsFrontiers(PathLengths* pathLengths, SinglePaths* singlePaths)
        : PathLengthsFrontiers(pathLengths), singlePaths{singlePaths} {}
    void beginFrontierComputeBetweenTables(table_id_t curFrontierTableID,
        table_id_t nextFrontierTableID) override {
        PathLengthsFrontiers::beginFrontierComputeBetweenTables(curFrontierTableID,
            nextFrontierTableID);
        singlePaths->fixNodeTable(nextFrontierTableID);
    }
    template<class TARGET>
    TARGET* ptrCast() {
        return common::ku_dynamic_cast<SinglePathsFrontiers*, TARGET*>(this);
    }

private:
    SinglePaths* singlePaths;
};

struct ShortestPathsBindData final : public GDSBindData {
    std::shared_ptr<Expression> nodeInput;
    uint8_t upperBound;

    ShortestPathsBindData(std::shared_ptr<Expression> nodeInput,
        std::shared_ptr<Expression> nodeOutput, bool outputAsNode, uint8_t upperBound)
        : GDSBindData{std::move(nodeOutput), outputAsNode}, nodeInput{std::move(nodeInput)},
          upperBound{upperBound} {
        KU_ASSERT(upperBound < 255);
    }
    ShortestPathsBindData(const ShortestPathsBindData& other)
        : GDSBindData{other}, nodeInput{other.nodeInput}, upperBound{other.upperBound} {}

    bool hasNodeInput() const override { return true; }
    std::shared_ptr<binder::Expression> getNodeInput() const override { return nodeInput; }

    std::unique_ptr<GDSBindData> copy() const override {
        return std::make_unique<ShortestPathsBindData>(*this);
    }
};

struct ShortestPathsSourceState {
    nodeID_t sourceNodeID;
    std::unique_ptr<PathLengths> pathLengths;
    std::unique_ptr<SinglePaths> singlePaths;
    explicit ShortestPathsSourceState(graph::Graph* graph, nodeID_t sourceNodeID,
        RecJoinOutputType outputType, MemoryManager* mm = nullptr)
        : sourceNodeID{sourceNodeID} {
        std::vector<std::tuple<common::table_id_t, uint64_t>> nodeTableIDAndNumNodes;
        for (common::table_id_t tableID : graph->getNodeTableIDs()) {
            auto numNodes = graph->getNumNodes(tableID);
            nodeTableIDAndNumNodes.push_back({tableID, numNodes});
        }
        pathLengths = std::make_unique<PathLengths>(nodeTableIDAndNumNodes);
        // We need to initialize the source as active in the frontier. Because PathLengths
        // is a single data structure that represents both the current and next frontier,
        // and because setting active is an operation done on the next frontier, we need to
        // fix the current frontier table before setting the source node as active.
        pathLengths->fixNextFrontierNodeTable(sourceNodeID.tableID);
        pathLengths->setActive(sourceNodeID);
        if (RecJoinOutputType::PATHS == outputType) {
            singlePaths = std::make_unique<SinglePaths>(nodeTableIDAndNumNodes, mm);
            // Unlike pathLengths we do not fix the nodeTable of singlePath with
            // sourceNodeID.tableID because when extending a RelTable R(srcNodeLabel, dstNodeLabel),
            // singlePaths should be fixed to dstNodeLabel. This should be done during frontier
            // extensions.
        }
    }
};

class ShortestPathsLocalState : public GDSLocalState {
public:
    explicit ShortestPathsLocalState(main::ClientContext* context, RecJoinOutputType outputType)
        : outputType{outputType} {
        auto mm = context->getMemoryManager();
        srcNodeIDVector = std::make_unique<ValueVector>(LogicalType::INTERNAL_ID(), mm);
        dstNodeIDVector = std::make_unique<ValueVector>(LogicalType::INTERNAL_ID(), mm);
        srcNodeIDVector->state = DataChunkState::getSingleValueDataChunkState();
        dstNodeIDVector->state = DataChunkState::getSingleValueDataChunkState();
        vectors.push_back(srcNodeIDVector.get());
        vectors.push_back(dstNodeIDVector.get());
        lengthVector = std::make_unique<ValueVector>(LogicalType::INT64(), mm);
        lengthVector->state = DataChunkState::getSingleValueDataChunkState();
        vectors.push_back(lengthVector.get());
        if (RecJoinOutputType::PATHS == outputType) {
            pathNodeIDsVector =
                std::make_unique<ValueVector>(LogicalType::LIST(LogicalType::INTERNAL_ID()), mm);
            pathNodeIDsVector->state = DataChunkState::getSingleValueDataChunkState();
            vectors.push_back(pathNodeIDsVector.get());
        }
    }

    void materialize(graph::Graph* graph, const ShortestPathsSourceState& sourceState,
        processor::FactorizedTable& table) const {
        srcNodeIDVector->setValue<nodeID_t>(0, sourceState.sourceNodeID);
        for (auto tableID : graph->getNodeTableIDs()) {
            sourceState.pathLengths->fixCurFrontierNodeTable(tableID);
            for (offset_t nodeOffset = 0;
                 nodeOffset < sourceState.pathLengths->getNumNodesInCurFrontierFixedNodeTable();
                 ++nodeOffset) {
                auto length =
                    sourceState.pathLengths->getMaskValueFromCurFrontierFixedMask(nodeOffset);
                if (length == PathLengths::UNVISITED) {
                    continue;
                }
                auto dstNodeID = nodeID_t{nodeOffset, tableID};
                dstNodeIDVector->setValue<nodeID_t>(0, dstNodeID);
                lengthVector->setValue<int64_t>(0, length);
                if (RecJoinOutputType::PATHS == outputType) {
                    nodeID_t curIntNode = dstNodeID;
                    uint64_t curIntNbrIndex = length > 1 ? length - 1 : 0;
                    pathNodeIDsVector->resetAuxiliaryBuffer();
                    auto pathNodeIDsEntry =
                        ListVector::addList(pathNodeIDsVector.get(), curIntNbrIndex);
                    pathNodeIDsVector->setValue(0, pathNodeIDsEntry);
                    auto dataVector = ListVector::getDataVector(pathNodeIDsVector.get());
                    while (curIntNbrIndex > 0) {
                        curIntNode = sourceState.singlePaths->getParent(curIntNode);
                        dataVector->setValue(pathNodeIDsEntry.offset + curIntNbrIndex - 1,
                            curIntNode);
                        curIntNbrIndex--;
                    }
                }
                table.append(vectors);
            }
        }
    }

private:
    RecJoinOutputType outputType;
    std::unique_ptr<ValueVector> srcNodeIDVector;
    std::unique_ptr<ValueVector> dstNodeIDVector;
    std::unique_ptr<ValueVector> lengthVector;
    std::unique_ptr<ValueVector> pathNodeIDsVector;
    std::vector<ValueVector*> vectors;
};

struct SPLengthsFrontierCompute : public FrontierCompute {
    PathLengthsFrontiers* pathLengthsFrontiers;
    explicit SPLengthsFrontierCompute(PathLengthsFrontiers* pathLengthsFrontiers)
        : pathLengthsFrontiers{pathLengthsFrontiers} {};

    bool edgeCompute(nodeID_t, nodeID_t nbrID) override {
        return pathLengthsFrontiers->pathLengths->getMaskValueFromNextFrontierFixedMask(
                   nbrID.offset) == PathLengths::UNVISITED;
    }
};

struct SPSinglePathsFrontierCompute : public FrontierCompute {
    SinglePathsFrontiers* singlePathsFrontiers;
    explicit SPSinglePathsFrontierCompute(SinglePathsFrontiers* singlePathsFrontiers)
        : singlePathsFrontiers{singlePathsFrontiers} {};

    bool edgeCompute(nodeID_t curNodeID, nodeID_t nbrID) override {
        auto retVal = singlePathsFrontiers->pathLengths->getMaskValueFromNextFrontierFixedMask(
                          nbrID.offset) == PathLengths::UNVISITED;
        if (retVal) {
            // We set the nbrID's last edge to curNodeID;
            singlePathsFrontiers->singlePaths->setLastEdge(nbrID, curNodeID);
        }
        return retVal;
    }
};

/**
 * Algorithm for parallel single shortest path computation, i.e., assumes Distinct semantics, so
 * multiplicities are ignored.
 */
class ShortestPathsAlgorithm final : public GDSAlgorithm {
    static constexpr char LENGTH_COLUMN_NAME[] = "length";
    static constexpr char PATH_NODE_IDS_COLUMN_NAME[] = "pathNodeIDs";

public:
    explicit ShortestPathsAlgorithm(RecJoinOutputType outputType) : outputType{outputType} {};
    ShortestPathsAlgorithm(const ShortestPathsAlgorithm& other)
        : GDSAlgorithm{other}, outputType{other.outputType} {}

    /*
     * Inputs are
     *
     * graph::ANY
     * srcNode::NODE
     * upperBound::INT64
     * outputProperty::BOOL
     */
    std::vector<common::LogicalTypeID> getParameterTypeIDs() const override {
        return {LogicalTypeID::ANY, LogicalTypeID::NODE, LogicalTypeID::INT64, LogicalTypeID::BOOL};
    }

    /*
     * Outputs are
     *
     * srcNode._id::INTERNAL_ID
     * _node._id::INTERNAL_ID (destination)
     * length::INT64
     */
    binder::expression_vector getResultColumns(binder::Binder* binder) const override {
        expression_vector columns;
        auto& inputNode = bindData->getNodeInput()->constCast<NodeExpression>();
        columns.push_back(inputNode.getInternalID());
        auto& outputNode = bindData->getNodeOutput()->constCast<NodeExpression>();
        columns.push_back(outputNode.getInternalID());
        columns.push_back(binder->createVariable(LENGTH_COLUMN_NAME, LogicalType::INT64()));
        if (RecJoinOutputType::PATHS == outputType) {
            columns.push_back(binder->createVariable(PATH_NODE_IDS_COLUMN_NAME,
                LogicalType::LIST(LogicalType::INTERNAL_ID())));
        }
        return columns;
    }

    void bind(const expression_vector& params, Binder* binder, GraphEntry& graphEntry) override {
        KU_ASSERT(params.size() == 4);
        auto nodeInput = params[1];
        auto nodeOutput = bindNodeOutput(binder, graphEntry);
        auto upperBound = ExpressionUtil::getLiteralValue<int64_t>(*params[2]);
        if (upperBound < 1 || upperBound >= 255) {
            throw RuntimeException(
                "shortest_paths function only works for positive max iterations that are up to "
                "254. Given upper bound is: " +
                std::to_string(upperBound) + ".");
        }
        auto outputProperty = ExpressionUtil::getLiteralValue<bool>(*params[3]);
        bindData = std::make_unique<ShortestPathsBindData>(nodeInput, nodeOutput, outputProperty,
            upperBound);
    }

    void initLocalState(main::ClientContext* context) override {
        localState = std::make_unique<ShortestPathsLocalState>(context, outputType);
    }

    void exec(processor::ExecutionContext* executionContext) override {
        for (auto& tableID : sharedState->graph->getNodeTableIDs()) {
            if (!sharedState->inputNodeOffsetMasks.contains(tableID)) {
                continue;
            }
            auto mask = sharedState->inputNodeOffsetMasks.at(tableID).get();
            for (auto offset = 0u; offset < sharedState->graph->getNumNodes(tableID); ++offset) {
                if (!mask->isMasked(offset, offset)) {
                    continue;
                }
                auto sourceNodeID = nodeID_t{offset, tableID};
                runShortestPathsFromSource(executionContext, sourceNodeID);
            }
        }
    }

    void runShortestPathsFromSource(processor::ExecutionContext* executionContext,
        nodeID_t sourceNodeID) {
        auto sourceState = ShortestPathsSourceState(sharedState->graph.get(), sourceNodeID,
            outputType, executionContext->clientContext->getMemoryManager());

        std::unique_ptr<Frontiers> frontiers;
        std::unique_ptr<FrontierCompute> frontierCompute;
        if (outputType == RecJoinOutputType::LENGTHS) {
            auto pathLengthsFrontier =
                std::make_unique<PathLengthsFrontiers>(sourceState.pathLengths.get());
            frontierCompute = std::make_unique<SPLengthsFrontierCompute>(pathLengthsFrontier.get());
            frontiers = std::move(pathLengthsFrontier);
        } else {
            auto singlePathsFrontier = std::make_unique<SinglePathsFrontiers>(
                sourceState.pathLengths.get(), sourceState.singlePaths.get());
            frontierCompute =
                std::make_unique<SPSinglePathsFrontierCompute>(singlePathsFrontier.get());
            frontiers = std::move(singlePathsFrontier);
        }
        GDSUtils::runFrontiersUntilConvergence(executionContext, *frontiers,
            sharedState->graph.get(), *frontierCompute,
            bindData->ptrCast<ShortestPathsBindData>()->upperBound);
        localState->ptrCast<ShortestPathsLocalState>()->materialize(sharedState->graph.get(),
            sourceState, *sharedState->fTable);
    }

    std::unique_ptr<GDSAlgorithm> copy() const override {
        return std::make_unique<ShortestPathsAlgorithm>(*this);
    }

private:
    RecJoinOutputType outputType;
};

function_set SingleSPLengthsFunction::getFunctionSet() {
    function_set result;
    auto function = std::make_unique<GDSFunction>(name,
        std::make_unique<ShortestPathsAlgorithm>(RecJoinOutputType::LENGTHS));
    result.push_back(std::move(function));
    return result;
}

function_set SingleSPPathsFunction::getFunctionSet() {
    function_set result;
    auto function = std::make_unique<GDSFunction>(name,
        std::make_unique<ShortestPathsAlgorithm>(RecJoinOutputType::PATHS));
    result.push_back(std::move(function));
    return result;
}

} // namespace function
} // namespace kuzu
