#pragma once

#include "function/gds/gds.h"
// TODO(Semih): Check if we can get rid of these
#include "function/gds/gds_frontier.h"
#include "processor/operator/mask.h"
// TODO(Semih): Try to remove after the SPOutputs refactor
#include "storage/buffer_manager/memory_manager.h"

// TODO(Semih): Remove
#include <iostream>

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
class EdgeCompute;

/**
 * A GDSFrontier implementation that keeps the lengths of the shortest paths from the source node to
 * destination nodes. This is a light-weight implementation that can keep lengths up to and
 * including UINT16_MAX - 1. The length stored for the source node is 0. Length UINT16_MAX is
 * reserved for marking nodes that are not visited. This class is intended to represent both the
 * current and next frontiers. At iteration i of the shortest path algorithm (iterations start
 * from 0), nodes with mask values i are in the current frontier. Nodes with any other values are
 * not in the frontier. Similarly at iteration i setting a node u active sets its mask value to i+1.
 * Therefore this class needs to be informed about the current iteration of the algorithm to
 * provide correct implementations of isActive and setActive functions.
 */
class PathLengths : public GDSFrontier {
    friend class PathLengthsFrontiers;
public:
    static constexpr uint16_t UNVISITED = UINT16_MAX;
    // curIter is the iteration number of the algorithm and starts from 0.
    // TODO(Semih): Make this private and make the reads happen through a function. That way
    // we can ensure that reads are always done through load(std::memory_order_relaxed).
    std::atomic<uint16_t> curIter = 0;

    explicit PathLengths(
        std::vector<std::tuple<common::table_id_t, uint64_t>> nodeTableIDAndNumNodes,
        storage::MemoryManager* mm);

    uint16_t getMaskValueFromCurFrontierFixedMask(common::offset_t nodeOffset) {
        return getCurFrontierFixedMask()[nodeOffset].load(std::memory_order_relaxed);
    }

    uint16_t getMaskValueFromNextFrontierFixedMask(common::offset_t nodeOffset) {
        return getNextFrontierFixedMask()[nodeOffset].load(std::memory_order_relaxed);
    }

    inline bool isActive(nodeID_t nodeID) override {
        auto curFrontierMask = getCurFrontierFixedMask();
        return curFrontierMask[nodeID.offset] == curIter.load(std::memory_order_relaxed) - 1;
    }

    inline void setActive(nodeID_t nodeID) override {
        auto nextFrontierMask = getNextFrontierFixedMask();
        // TODO(Semih): We should be removing this check because we assume that whether the active
        // check is done or not was done by the caller.
        if (nextFrontierMask[nodeID.offset].load(std::memory_order_relaxed) == UNVISITED) {
            nextFrontierMask[nodeID.offset].store(curIter.load(std::memory_order_relaxed),
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
    std::atomic<uint16_t>* getCurFrontierFixedMask() {
        auto retVal = curFrontierFixedMask.load(std::memory_order_relaxed);
        KU_ASSERT(retVal != nullptr);
        return retVal;
    }

    std::atomic<uint16_t>* getNextFrontierFixedMask() {
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
    // TODO(Semih): Remove current and next terminology from here because it is confusing if this
    // class is used to represent a single frontier (e.g., in VarLenJoins). Instead use
    // readActiveMask and setActiveMask.
    std::atomic<std::atomic<uint16_t>*> curFrontierFixedMask;
    std::atomic<std::atomic<uint16_t>*> nextFrontierFixedMask;
};

// TODO(Semih): Rename to SinglePathLengthsFrontiers and write documentation
class PathLengthsFrontiers : public Frontiers {
    friend struct AllSPLengthsEdgeCompute;
    friend struct AllSPPathsEdgeCompute;
    friend struct SingleSPLengthsEdgeCompute;
    friend struct SingleSPPathsEdgeCompute;
    // TODO(Semih): Remove
    friend struct VarLenJoinsEdgeCompute;

public:
    explicit PathLengthsFrontiers(std::shared_ptr<PathLengths> pathLengths, uint64_t maxThreadsForExec)
        : Frontiers(pathLengths /* curFrontier */, pathLengths /* nextFrontier */,
              1 /* initial num active nodes */, maxThreadsForExec),
          pathLengths{pathLengths} {
        morselizer = std::make_unique<FrontierMorselizer>(maxThreadsForExec);
    }

    bool getNextRangeMorsel(FrontierMorsel& frontierMorsel) override;

    void initRJFromSource(nodeID_t source) override {
        // Because PathLengths is a single data structure that represents both the current and next
        // frontier, and because setting active is an operation done on the next frontier, we first
        // fix the next frontier table before setting the source node as active.
        pathLengths->fixNextFrontierNodeTable(source.tableID);
        pathLengths->setActive(source);
    }

    void beginFrontierComputeBetweenTables(table_id_t curFrontierTableID,
        table_id_t nextFrontierTableID) override;

    void beginNewIterationInternalNoLock() override {
        pathLengths->incrementCurIter();
    }

private:
    std::shared_ptr<PathLengths> pathLengths;
    std::unique_ptr<FrontierMorselizer> morselizer;
};

class DoublePathLengthsFrontiers : public Frontiers {
    friend struct VarLenJoinsEdgeCompute;

public:
    // TODO(Semih): Check if mm should be defaulted to null. Also the constructor is in cpp. Be
    // consistent with SinglePathLengthsFrontiers in the style.
    explicit DoublePathLengthsFrontiers(std::vector<std::tuple<common::table_id_t, uint64_t>> nodeTableIDAndNumNodes,
        uint64_t maxThreadsForExec, storage::MemoryManager* mm = nullptr);

    bool getNextRangeMorsel(FrontierMorsel& frontierMorsel) override;

    void initRJFromSource(nodeID_t source) override {
        // TODO(Semih): Check if we need fixNextFrontierNodeTable call here because the following
        // commment existed in SinglePathLengthsFrontiers::initRJFromSource: "Because PathLengths is
        // a single data structure that represents both the current and next frontier, and because
        // setting active is an operation done on the next frontier, we first fix the next frontier
        // table before setting the source node as active."
        nextFrontier->ptrCast<PathLengths>()->fixNextFrontierNodeTable(source.tableID);
        nextFrontier->ptrCast<PathLengths>()->setActive(source);
    }

    void beginFrontierComputeBetweenTables(table_id_t curFrontierTableID,
        table_id_t nextFrontierTableID) override;

    void beginNewIterationInternalNoLock() override {
        curFrontier->ptrCast<PathLengths>()->incrementCurIter();
        nextFrontier->ptrCast<PathLengths>()->incrementCurIter();
    }

private:
    std::unique_ptr<FrontierMorselizer> morselizer;
};

struct RJBindData final : public function::GDSBindData {
    static constexpr uint16_t DEFAULT_MAXIMUM_ALLOWED_UPPER_BOUND = (uint16_t) 255;

    std::shared_ptr<binder::Expression> nodeInput;
    // Important Note: For any recursive join algorithm other than variable length joins, lower bound must
    // always be 1. For variable length joins, lower bound 0 has a special meaning, for which
    // we follow the behavior of Neo4j. Lower bound 0 means that when matching the (a)-[e*0..max]->(b)
    // or its variant forms, such as (a)-[*0..max]-(b), if no such path exists between possible
    // bound a nodes and b nodes, say between nodes s and d, such that s matches all filters on node pattern
    // a and b matches all filters on node pattern b, we should still return an empty path, i.e., one that binds
    // no edges to e. If there is a non-empty path between s and d of length > 1, then we don't return the empty path
    // in addition to non-empty paths. So the semantics is that of optional match.
    uint16_t lowerBound;
    uint16_t upperBound;

    RJBindData(std::shared_ptr<binder::Expression> nodeInput,
        std::shared_ptr<binder::Expression> nodeOutput, bool outputAsNode, uint16_t lowerBound, uint16_t upperBound)
        : GDSBindData{std::move(nodeOutput), outputAsNode}, nodeInput{std::move(nodeInput)},
          lowerBound{lowerBound}, upperBound{upperBound} {
        KU_ASSERT(upperBound < DEFAULT_MAXIMUM_ALLOWED_UPPER_BOUND);
    }
    RJBindData(const RJBindData& other)
        : GDSBindData{other}, nodeInput{other.nodeInput}, lowerBound{other.lowerBound}, upperBound{other.upperBound} {}

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

// TODO(Semih): PathLengths should be a member of a new class SPOutputs. It could also be a
// raw pointer, where the unique_ptr is stored in SinglePathLengthsFrontiers. Also document
// the SinglePathLengthsFrontiers optimization.
struct RJOutputs {
    // TODO(Semih): Refactor sourceNodeID and pathLengths into RJOutputs.
    nodeID_t sourceNodeID;
    // TODO(Semih): Make private or move somewhere else
    std::vector<std::tuple<common::table_id_t, uint64_t>> nodeTableIDAndNumNodes;

public:
    RJOutputs(graph::Graph* graph, nodeID_t sourceNodeID);
    virtual ~RJOutputs() = default;

    virtual void initRJFromSource(nodeID_t) {}
    virtual void beginFrontierComputeBetweenTables(table_id_t curFrontierTableID,
        table_id_t nextFrontierTableID) = 0;
    template<class TARGET>
    TARGET* ptrCast() {
        return common::ku_dynamic_cast<RJOutputs*, TARGET*>(this);
    }
};

struct SPOutputs : public RJOutputs {
    std::shared_ptr<PathLengths> pathLengths;
    // TODO(Semih): Check if mm should be defaulted to nullptr.
    SPOutputs(graph::Graph* graph, nodeID_t sourceNodeID, storage::MemoryManager* mm = nullptr);
    // TODO(Semih/Reviewer): What should I do for default destructors generally?
};
/**
 * Helper class to write paths to a ValueVector. The ValueVector should be a ListVector. The path
 * is written in reverse order, so the calls to addNewNodeID should be done in the reverse order of
 * the nodes that should be placed into the ListVector.
 */
class PathVectorWriter {
public:
    explicit PathVectorWriter(common::ValueVector* pathNodeIDsVector) : pathNodeIDsVector{pathNodeIDsVector} {}
    void beginWritingNewPath(uint64_t length);
    void addNewNodeID(nodeID_t curIntNode);

public:
    common::ValueVector* pathNodeIDsVector;
    list_entry_t curPathListEntry;
    uint64_t nextPathPos;
};

class RJOutputWriter {
public:
    // TODO(Semih): Consider changing raw pointers with shared pointers
    explicit RJOutputWriter(main::ClientContext* contex, RJOutputs* rjOutputs);
    virtual ~RJOutputWriter() = default;

    // This is called once by the "master" thread.
    void initWritingFromSource(nodeID_t sourceNodeID);

    // TODO(Semih): Have this function implemented in RJOutputs. Then don't make
    // RJOutputWriter::beginWritingForDstNodesInTable be a virtual class but instead make it
    // directly call RJOutputs::beginWritingForDstNodesInTable. Also make RJOutputWriter keep the
    // RJOutputs as a field so we don't need to keep passing it as an argument to skipWriting and
    // write functions.
    virtual void beginWritingForDstNodesInTable(RJOutputs* rjOutputs, table_id_t tableID) const = 0;

    virtual bool skipWriting(RJOutputs* rjOutputs, nodeID_t dstNodeID) const = 0;

    virtual void write(RJOutputs* rjOutputs,
        processor::FactorizedTable& fTable, nodeID_t dstNodeID) const = 0;

    virtual std::unique_ptr<RJOutputWriter> clone() = 0;
protected:
    main::ClientContext* context;
    RJOutputs* rjOutputs;
    std::unique_ptr<common::ValueVector> srcNodeIDVector;
    std::unique_ptr<common::ValueVector> dstNodeIDVector;
    std::vector<common::ValueVector*> vectors;
};

// Wrapper around the data that needs to be stored during the computation of a recursive joins
// computation from one source. Also contains several initialization functions.
struct RJCompState {
    std::unique_ptr<function::Frontiers> frontiers;
    std::unique_ptr<function::EdgeCompute> edgeCompute;
    std::unique_ptr<RJOutputs> outputs;
    std::unique_ptr<RJOutputWriter> outputWriter;

    explicit RJCompState(std::unique_ptr<function::Frontiers> frontiers,
        std::unique_ptr<function::EdgeCompute> frontierCompute,
        std::unique_ptr<RJOutputs> outputs,
        std::unique_ptr<RJOutputWriter> outputWriter);

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

class SPOutputWriterDsts : public RJOutputWriter {
public:
    explicit SPOutputWriterDsts(main::ClientContext* context, RJOutputs* rjOutputs) : RJOutputWriter(context, rjOutputs) {};

    void write(RJOutputs* rjOutputs,
        processor::FactorizedTable& fTable, nodeID_t dstNodeID) const override;

    void beginWritingForDstNodesInTable(RJOutputs* rjOutputs, table_id_t tableID) const override {
        rjOutputs->ptrCast<SPOutputs>()->pathLengths->fixCurFrontierNodeTable(tableID);
        fixOtherStructuresToOutputDstsFromNodeTable(rjOutputs, tableID);
    }

    bool skipWriting(RJOutputs* rjOutputs, nodeID_t dstNodeID) const override {
        return dstNodeID == rjOutputs->ptrCast<SPOutputs>()->sourceNodeID || PathLengths::UNVISITED ==
            rjOutputs->ptrCast<SPOutputs>()->pathLengths->getMaskValueFromCurFrontierFixedMask(dstNodeID.offset);
    }

    std::unique_ptr<RJOutputWriter> clone() override {
        return std::make_unique<SPOutputWriterDsts>(context, rjOutputs);
    }
protected:
    virtual void fixOtherStructuresToOutputDstsFromNodeTable(RJOutputs*, table_id_t) const {}

    virtual void writeMoreAndAppend(
        processor::FactorizedTable& fTable, RJOutputs* rjOutputs, nodeID_t dstNodeID, uint16_t length) const;
};

class RJAlgorithm : public GDSAlgorithm {
    static constexpr char LENGTH_COLUMN_NAME[] = "length";
    static constexpr char PATH_NODE_IDS_COLUMN_NAME[] = "pathNodeIDs";

public:
    explicit RJAlgorithm(RJOutputType outputType, bool hasLowerBoundInput = false)
        : outputType{outputType}, hasLowerBoundInput{hasLowerBoundInput} {};
    RJAlgorithm(const RJAlgorithm& other) : GDSAlgorithm{other}, outputType{other.outputType}, hasLowerBoundInput{other.hasLowerBoundInput} {}
    // TODO(Reviewer): Do I have to do this? We should make sure we're being safe here.
    virtual ~RJAlgorithm() = default;
    /*
     * Inputs include the following:
     *
     * graph::ANY
     * srcNode::NODE
     * lowerBound::INT64 (optional)
     * upperBound::INT64
     * outputProperty::BOOL
     */
    std::vector<common::LogicalTypeID> getParameterTypeIDs() const override {
        if (hasLowerBoundInput) {
            return {common::LogicalTypeID::ANY, common::LogicalTypeID::NODE,
                common::LogicalTypeID::INT64, common::LogicalTypeID::INT64, common::LogicalTypeID::BOOL};
        } else {
            return {common::LogicalTypeID::ANY, common::LogicalTypeID::NODE,
                common::LogicalTypeID::INT64, common::LogicalTypeID::BOOL};
        }
    }

    /*
     * Outputs should include at least the following (but possibly other fields, e.g., lengths or
     * paths): srcNode._id::INTERNAL_ID _node._id::INTERNAL_ID (destination)
     */
    binder::expression_vector getResultColumns(binder::Binder* binder) const override;

    void bind(const binder::expression_vector& params, binder::Binder* binder,
        graph::GraphEntry& graphEntry) override;

    void exec(processor::ExecutionContext* executionContext) override;
    virtual std::unique_ptr<RJCompState> getRJCompState(
        processor::ExecutionContext* executionContext, nodeID_t sourceNodeID) = 0;

protected:
    RJOutputType outputType;
    bool hasLowerBoundInput;
};
} // namespace function
} // namespace kuzu