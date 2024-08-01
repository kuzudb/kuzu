#pragma once

#include "function/gds/gds.h"
// TODO(Semih): Check if we can get rid of these
#include "function/gds/gds_frontier.h"
#include "processor/operator/mask.h"

namespace kuzu {
namespace common {
class ValueVector;
} // namespace common

namespace graph {
class Graph;
} // namespace graph

namespace storage {
class MemoryBuffer;
class MemoryManager;
} // namespace storage
namespace function {
class Frontiers;
class FrontierCompute;

struct RJBindData final : public function::GDSBindData {
    std::shared_ptr<binder::Expression> nodeInput;
    uint8_t upperBound;

    RJBindData(std::shared_ptr<binder::Expression> nodeInput,
        std::shared_ptr<binder::Expression> nodeOutput, bool outputAsNode, uint8_t upperBound)
        : GDSBindData{std::move(nodeOutput), outputAsNode}, nodeInput{std::move(nodeInput)},
          upperBound{upperBound} {
        KU_ASSERT(upperBound < 255);
    }
    RJBindData(const RJBindData& other)
        : GDSBindData{other}, nodeInput{other.nodeInput}, upperBound{other.upperBound} {}

    bool hasNodeInput() const override { return true; }
    std::shared_ptr<binder::Expression> getNodeInput() const override { return nodeInput; }

    std::unique_ptr<GDSBindData> copy() const override {
        return std::make_unique<RJBindData>(*this);
    }
};

enum class RJOutputType : uint8_t {
    DESTINATION_NODES = 0,
    LENGTHS = 1,
    // PATHS returns only the intermediate nodeIDs in a path, not the source or dst nodeIDs.
    PATHS = 2,
};

struct RJOutputs {
    // Refactor sourceNodeID and pathLengths into RJOutputs.
    nodeID_t sourceNodeID;

public:
    RJOutputs(nodeID_t sourceNodeID, RJOutputType outputType)
        : sourceNodeID{sourceNodeID}, outputType{outputType} {}
    virtual ~RJOutputs() = default;

    virtual void initRJFromSource(nodeID_t source) {}
    virtual void beginFrontierComputeBetweenTables(table_id_t curFrontierTableID,
        table_id_t nextFrontierTableID) = 0;
    template<class TARGET>
    TARGET* ptrCast() {
        return common::ku_dynamic_cast<RJOutputs*, TARGET*>(this);
    }

protected:
    RJOutputType outputType;
};

// TODO(Semih): Turn this into a scanner if we can find a way to pipeline the outputs to the
// next operators in the plan.
class RJOutputWriter {
public:
    explicit RJOutputWriter(main::ClientContext* context, RJOutputType outputType);
    virtual ~RJOutputWriter() = default;

    virtual void materialize(graph::Graph* graph, RJOutputs* rjOutputs,
        processor::FactorizedTable& fTable) const = 0;

protected:
    RJOutputType rjOutputType;
    std::unique_ptr<common::ValueVector> srcNodeIDVector;
    std::unique_ptr<common::ValueVector> dstNodeIDVector;
    std::unique_ptr<common::ValueVector> lengthVector;
    std::unique_ptr<common::ValueVector> pathNodeIDsVector;
    std::vector<common::ValueVector*> vectors;
};

// Wrapper around the data that needs to be stored during the computation of a recursive joins
// computation from one source. Also contains several initialization functions.
struct RJCompState {
    std::unique_ptr<RJOutputs> outputs;
    std::unique_ptr<function::Frontiers> frontiers;
    std::unique_ptr<function::FrontierCompute> frontierCompute;

    explicit RJCompState(std::unique_ptr<RJOutputs> outputs,
        std::unique_ptr<function::Frontiers> frontiers,
        std::unique_ptr<function::FrontierCompute> frontierCompute);

    void initRJFromSource(nodeID_t sourceNodeID) {
        frontiers->initRJFromSource(sourceNodeID);
        outputs->initRJFromSource(sourceNodeID);
    }
    // When performing computations on multi-label graphs, it may be beneficial to fix a single
    // node table of nodes in the current frontier and a single node table of nodes for the next
    // frontier. That is because algorithms will generally perform extensions using a single
    // relationship table at a time, and each relationship table R is between a single source node
    // table S and a single destination node table T. Therefore, generally during execution the
    // algorithm will need to check only the active S nodes in current frontier and update the
    // active statuses of only the T nodes in the next frontier. The information that the algorithm
    // is beginning and S-to-T extensions can be given to the computation to possibly avoid them
    // doing lookups of S and T-related data structures, e.g., masks, internally.
    void beginFrontierComputeBetweenTables(table_id_t curFrontierTableID,
        table_id_t nextFrontierTableID) {
        frontiers->beginFrontierComputeBetweenTables(curFrontierTableID, nextFrontierTableID);
        outputs->beginFrontierComputeBetweenTables(curFrontierTableID, nextFrontierTableID);
    }
};

class RJAlgorithm : public function::GDSAlgorithm {
    static constexpr char LENGTH_COLUMN_NAME[] = "length";
    static constexpr char PATH_NODE_IDS_COLUMN_NAME[] = "pathNodeIDs";

public:
    explicit RJAlgorithm(RJOutputType outputType) : outputType{outputType} {};
    RJAlgorithm(const RJAlgorithm& other) : GDSAlgorithm{other}, outputType{other.outputType} {}
    // TODO(Reviewer): Do I have to do this? We should make sure we're being safe here.
    virtual ~RJAlgorithm() = default;
    /*
     * Inputs include the following:
     *
     * graph::ANY
     * srcNode::NODE
     * upperBound::INT64
     * outputProperty::BOOL
     */
    std::vector<common::LogicalTypeID> getParameterTypeIDs() const override {
        return {common::LogicalTypeID::ANY, common::LogicalTypeID::NODE,
            common::LogicalTypeID::INT64, common::LogicalTypeID::BOOL};
    }

    /*
     * Outputs should include at least the following (but possibly other fields, e.g., lengths or
     * paths): srcNode._id::INTERNAL_ID _node._id::INTERNAL_ID (destination)
     */
    binder::expression_vector getResultColumns(binder::Binder* binder) const override;

    void bind(const binder::expression_vector& params, binder::Binder* binder,
        graph::GraphEntry& graphEntry) override;

    void exec(processor::ExecutionContext* executionContext) override;
    virtual std::unique_ptr<RJCompState> getFrontiersAndFrontiersCompute(
        processor::ExecutionContext* executionContext, common::nodeID_t sourceNodeID) = 0;

protected:
    RJOutputType outputType;
    std::unique_ptr<RJOutputWriter> outputWriter;
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
    // TODO(Semih): Make this private and make the reads happen through a function. That way
    // we can ensure that reads are always done through load(std::memory_order_relaxed).
    std::atomic<uint8_t> curIter = 255;

    explicit PathLengths(
        std::vector<std::tuple<common::table_id_t, uint64_t>> nodeTableIDAndNumNodes,
        storage::MemoryManager* mm);

    uint8_t getMaskValueFromCurFrontierFixedMask(common::offset_t nodeOffset) {
        return getCurFrontierFixedMask()[nodeOffset].load(std::memory_order_relaxed);
    }

    uint8_t getMaskValueFromNextFrontierFixedMask(common::offset_t nodeOffset) {
        return getNextFrontierFixedMask()[nodeOffset].load(std::memory_order_relaxed);
    }

    inline bool isActive(nodeID_t nodeID) override {
        auto curFrontierMask = getCurFrontierFixedMask();
        return curFrontierMask[nodeID.offset] == curIter.load(std::memory_order_relaxed);
    }

    inline void setActive(nodeID_t nodeID) override {
        auto nextFrontierMask = getNextFrontierFixedMask();
        if (nextFrontierMask[nodeID.offset].load(std::memory_order_relaxed) == UNVISITED) {
            // Note that if curIter = 255, this will set the mask value to 0. Therefore when
            // the next (and first) iteration of the algorithm starts, the node will be "active".
            nextFrontierMask[nodeID.offset].store(curIter.load(std::memory_order_relaxed) + 1,
                std::memory_order_relaxed);
        }
    }

    void incrementCurIter() { curIter.fetch_add(1, std::memory_order_relaxed); }

    void fixCurFrontierNodeTable(common::table_id_t tableID);

    void fixNextFrontierNodeTable(common::table_id_t tableID);

    uint64_t getNumNodesInCurFrontierFixedNodeTable() {
        KU_ASSERT(curFrontierFixedMask.load(std::memory_order_relaxed) != nullptr);
        return maxNodesInCurFrontierFixedMask.load(std::memory_order_relaxed);
    }
private:
    std::atomic<uint8_t>* getCurFrontierFixedMask() {
        auto retVal = curFrontierFixedMask.load(std::memory_order_relaxed);
        KU_ASSERT(retVal != nullptr);
        return retVal;
    }

    std::atomic<uint8_t>* getNextFrontierFixedMask() {
        auto retVal = nextFrontierFixedMask.load(std::memory_order_relaxed);
        KU_ASSERT(retVal != nullptr);
        return retVal;
    }
private:
    // We do not need to make nodeTableIDAndNumNodesMap and masks atomic because they should only
    // be accessed by functions that are called by the "master GDS thread" (so not accessed inside
    // the parallel functions in GDSUtils.
    std::unordered_map<common::table_id_t, uint64_t> nodeTableIDAndNumNodesMap;
    common::table_id_map_t<std::unique_ptr<storage::MemoryBuffer>> masks;
    std::atomic<table_id_t> curTableID;
    std::atomic<uint64_t> maxNodesInCurFrontierFixedMask;
    std::atomic<std::atomic<uint8_t>*> curFrontierFixedMask;
    std::atomic<std::atomic<uint8_t>*> nextFrontierFixedMask;
};

class PathLengthsFrontiers : public Frontiers {
    friend struct AllSPLengthsFrontierCompute;
    friend struct AllSPPathsFrontierCompute;
    friend struct SingleSPLengthsFrontierCompute;
    friend struct SingleSPPathsFrontierCompute;
    static constexpr uint64_t MIN_FRONTIER_MORSEL_SIZE = 512;
    // Note: MIN_NUMBER_OF_FRONTIER_MORSELS is the minimum number of morsels we aim to have but we
    // can have fewer than this. See the beginFrontierComputeBetweenTables to see the actual
    // frontierMorselSize computation for details.
    static constexpr uint64_t MIN_NUMBER_OF_FRONTIER_MORSELS = 128;

public:
    explicit PathLengthsFrontiers(PathLengths* pathLengths, uint64_t maxThreadsForExec)
        : Frontiers(pathLengths /* curFrontier */, pathLengths /* nextFrontier */,
              1 /* initial num active nodes */, maxThreadsForExec),
          pathLengths{pathLengths} {}

    bool getNextFrontierMorsel(RangeFrontierMorsel& frontierMorsel) override;

    // TODO(Semih): Rename initSP to initRJFromSource
    void initRJFromSource(nodeID_t source) override {
        // Because PathLengths is a single data structure that represents both the current and next
        // frontier, and because setting active is an operation done on the next frontier, we first
        // fix the next frontier table before setting the source node as active.
        pathLengths->fixNextFrontierNodeTable(source.tableID);
        pathLengths->setActive(source);
    }

    void beginFrontierComputeBetweenTables(table_id_t curFrontierTableID,
        table_id_t nextFrontierTableID) override {
        pathLengths->fixCurFrontierNodeTable(curFrontierTableID);
        pathLengths->fixNextFrontierNodeTable(nextFrontierTableID);
        nextOffset.store(0u);
        // Frontier size calculation: The ideal scenario is to have k^2 many morsels where k
        // the number of maximum threads that could be working on this frontier. However if
        // that is too small then we default to MIN_FRONTIER_MORSEL_SIZE.
        auto maxNodesInFrontier = pathLengths->getNumNodesInCurFrontierFixedNodeTable();
        auto idealFrontierMorselSize = maxNodesInFrontier / (std::max(MIN_NUMBER_OF_FRONTIER_MORSELS,
                                                          maxThreadsForExec * maxThreadsForExec));
        frontierMorselSize = std::max(MIN_FRONTIER_MORSEL_SIZE, idealFrontierMorselSize);
    }

    void beginNewIterationInternalNoLock() override {
        nextOffset.store(0u);
        pathLengths->incrementCurIter();
    }

private:
    PathLengths* pathLengths;
    std::atomic<offset_t> nextOffset;
    uint64_t frontierMorselSize;
};

} // namespace function
} // namespace kuzu