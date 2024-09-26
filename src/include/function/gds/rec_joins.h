#pragma once

#include "function/gds/gds.h"
#include "function/gds/gds_frontier.h"

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
class EdgeCompute;
class FrontierPair;
class GDSFrontier;

/**
 * A GDSFrontier implementation that keeps the lengths of the paths from a source node to
 * destination nodes. This is a light-weight implementation that can keep lengths up to and
 * including UINT16_MAX - 1. The length stored for the source node is 0. Length UINT16_MAX is
 * reserved for marking nodes that are not visited. The lengths stored per node are effectively the
 * iteration numbers that a node is visited. For example, if the running computation is shortest
 * path, then the length stored is the shortest path length.
 *
 * Note: This class can be used to represent both the current and next frontierPair for shortest
 * paths computations, which have the guarantee that a vertex is part of only one frontier level.
 * Specifically, at iteration i of the shortest path algorithm (iterations start from 0), nodes with
 * mask values i are in the current frontier. Nodes with any other values are not in the frontier.
 * Similarly at iteration i setting a node u active sets its mask value to i+1. To enable this
 * usage, this class contains functions to expose two frontierPair to users, e.g.,
 * getMaskValueFromCur/NextFrontierFixedMask. In the case of shortest path computations, using this
 * class to represent two frontierPair should be faster or take less space than keeping two separate
 * frontierPair.
 *
 * However, this is not necessary and the caller can also use this to represent a single frontier.
 */
class PathLengths : public GDSFrontier {
    friend class SinglePathLengthsFrontierPair;

public:
    static constexpr uint16_t UNVISITED = UINT16_MAX;

    explicit PathLengths(std::unordered_map<common::table_id_t, uint64_t> nodeTableIDAndNumNodes,
        storage::MemoryManager* mm);

    uint16_t getMaskValueFromCurFrontierFixedMask(common::offset_t nodeOffset) {
        return getCurFrontierFixedMask()[nodeOffset].load(std::memory_order_relaxed);
    }

    uint16_t getMaskValueFromNextFrontierFixedMask(common::offset_t nodeOffset) {
        return getNextFrontierFixedMask()[nodeOffset].load(std::memory_order_relaxed);
    }

    inline bool isActive(common::nodeID_t nodeID) override {
        return getCurFrontierFixedMask()[nodeID.offset] ==
               curIter.load(std::memory_order_relaxed) - 1;
    }

    inline void setActive(common::nodeID_t nodeID) override {
        getNextFrontierFixedMask()[nodeID.offset].store(curIter.load(std::memory_order_relaxed),
            std::memory_order_relaxed);
    }

    void incrementCurIter() { curIter.fetch_add(1, std::memory_order_relaxed); }

    void fixCurFrontierNodeTable(common::table_id_t tableID);

    void fixNextFrontierNodeTable(common::table_id_t tableID);

    uint64_t getNumNodesInCurFrontierFixedNodeTable() {
        KU_ASSERT(curFrontierFixedMask.load(std::memory_order_relaxed) != nullptr);
        return maxNodesInCurFrontierFixedMask.load(std::memory_order_relaxed);
    }

    uint16_t getCurIter() { return curIter.load(std::memory_order_relaxed); }

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
    // the parallel functions in GDSUtils, which are called by other "worker threads").
    std::unordered_map<common::table_id_t, uint64_t> nodeTableIDAndNumNodesMap;
    common::table_id_map_t<std::unique_ptr<storage::MemoryBuffer>> masks;
    // See FrontierPair::curIter. We keep a copy of curIter here because PathLengths stores
    // iteration numbers for vertices and uses them to identify which vertex is in the frontier.
    std::atomic<uint16_t> curIter;
    std::atomic<table_id_t> curTableID;
    std::atomic<uint64_t> maxNodesInCurFrontierFixedMask;
    std::atomic<std::atomic<uint16_t>*> curFrontierFixedMask;
    std::atomic<std::atomic<uint16_t>*> nextFrontierFixedMask;
};

class SinglePathLengthsFrontierPair : public FrontierPair {
    friend struct AllSPLengthsEdgeCompute;
    friend struct AllSPPathsEdgeCompute;
    friend struct SingleSPLengthsEdgeCompute;
    friend struct SingleSPPathsEdgeCompute;

public:
    explicit SinglePathLengthsFrontierPair(std::shared_ptr<PathLengths> pathLengths,
        uint64_t maxThreadsForExec)
        : FrontierPair(pathLengths /* curFrontier */, pathLengths /* nextFrontier */,
              1 /* initial num active nodes */, maxThreadsForExec),
          pathLengths{pathLengths}, morselDispatcher(maxThreadsForExec) {}

    bool getNextRangeMorsel(FrontierMorsel& frontierMorsel) override;

    void initRJFromSource(nodeID_t source) override;

    void beginFrontierComputeBetweenTables(table_id_t curFrontierTableID,
        table_id_t nextFrontierTableID) override;

    void beginNewIterationInternalNoLock() override { pathLengths->incrementCurIter(); }

private:
    std::shared_ptr<PathLengths> pathLengths;
    FrontierMorselDispatcher morselDispatcher;
};

class DoublePathLengthsFrontierPair : public FrontierPair {
    friend struct VarLenJoinsEdgeCompute;

public:
    explicit DoublePathLengthsFrontierPair(
        std::unordered_map<common::table_id_t, uint64_t> nodeTableIDAndNumNodes,
        uint64_t maxThreadsForExec, storage::MemoryManager* mm);

    bool getNextRangeMorsel(FrontierMorsel& frontierMorsel) override;

    void initRJFromSource(nodeID_t source) override;

    void beginFrontierComputeBetweenTables(table_id_t curFrontierTableID,
        table_id_t nextFrontierTableID) override;

    void beginNewIterationInternalNoLock() override {
        curFrontier->ptrCast<PathLengths>()->incrementCurIter();
        nextFrontier->ptrCast<PathLengths>()->incrementCurIter();
    }

private:
    std::unique_ptr<FrontierMorselDispatcher> morselDispatcher;
};

struct RJBindData final : public function::GDSBindData {
    static constexpr uint16_t DEFAULT_MAXIMUM_ALLOWED_UPPER_BOUND = (uint16_t)255;

    std::shared_ptr<binder::Expression> nodeInput;
    // Important Note: For any recursive join algorithm other than variable length joins, lower
    // bound must always be 1. For variable length joins, lower bound 0 has a special meaning, for
    // which we follow the behavior of Neo4j. Lower bound 0 means that when matching the
    // (a)-[e*0..max]->(b) or its variant forms, such as (a)-[*0..max]-(b), if no such path exists
    // between possible bound a nodes and b nodes, say between nodes s and d, such that s matches
    // all filters on node pattern a and b matches all filters on node pattern b, we should still
    // return an empty path, i.e., one that binds no edges to e. If there is a non-empty path
    // between s and d of length > 1, then we don't return the empty path in addition to non-empty
    // paths. So the semantics is that of optional match.
    uint16_t lowerBound;
    uint16_t upperBound;

    RJBindData(std::shared_ptr<binder::Expression> nodeInput,
        std::shared_ptr<binder::Expression> nodeOutput, bool outputAsNode, uint16_t lowerBound,
        uint16_t upperBound)
        : GDSBindData{std::move(nodeOutput), outputAsNode}, nodeInput{std::move(nodeInput)},
          lowerBound{lowerBound}, upperBound{upperBound} {
        KU_ASSERT(upperBound < DEFAULT_MAXIMUM_ALLOWED_UPPER_BOUND);
    }
    RJBindData(const RJBindData& other)
        : GDSBindData{other}, nodeInput{other.nodeInput}, lowerBound{other.lowerBound},
          upperBound{other.upperBound} {}

    bool hasNodeInput() const override { return true; }
    std::shared_ptr<binder::Expression> getNodeInput() const override { return nodeInput; }

    std::unique_ptr<GDSBindData> copy() const override {
        return std::make_unique<RJBindData>(*this);
    }
};

struct RJOutputs {
public:
    explicit RJOutputs(nodeID_t sourceNodeID);
    virtual ~RJOutputs() = default;

    virtual void initRJFromSource(nodeID_t) {}
    virtual void beginFrontierComputeBetweenTables(table_id_t curFrontierTableID,
        table_id_t nextFrontierTableID) = 0;
    // This function is called after the recursive join computation stage, at the stage when the
    // outputs that are stored in RJOutputs is being written to FactorizedTable.
    virtual void beginWritingOutputsForDstNodesInTable(table_id_t tableID) = 0;
    template<class TARGET>
    TARGET* ptrCast() {
        return common::ku_dynamic_cast<TARGET*>(this);
    }

public:
    nodeID_t sourceNodeID;
};

struct SPOutputs : public RJOutputs {
public:
    explicit SPOutputs(std::unordered_map<common::table_id_t, uint64_t> nodeTableIDAndNumNodes,
        nodeID_t sourceNodeID, storage::MemoryManager* mm);

public:
    std::shared_ptr<PathLengths> pathLengths;
};
/**
 * Helper class to write paths to a ValueVector. The ValueVector should be a ListVector. The path
 * is written in reverse order, so the calls to addNewNodeID should be done in the reverse order of
 * the nodes that should be placed into the ListVector.
 */
class PathVectorWriter {
public:
    explicit PathVectorWriter(common::ValueVector* pathNodeIDsVector)
        : pathNodeIDsVector{pathNodeIDsVector}, curPathListEntry{}, nextPathPos{0} {}
    void beginWritingNewPath(uint64_t length);
    void addNewNodeID(nodeID_t curIntNode);

public:
    common::ValueVector* pathNodeIDsVector;
    list_entry_t curPathListEntry;
    uint64_t nextPathPos;
};

class RJOutputWriter {
public:
    explicit RJOutputWriter(main::ClientContext* contex, RJOutputs* rjOutputs);
    virtual ~RJOutputWriter() = default;

    void beginWritingForDstNodesInTable(table_id_t tableID) {
        rjOutputs->beginWritingOutputsForDstNodesInTable(tableID);
    }

    virtual bool skipWriting(nodeID_t dstNodeID) const = 0;

    virtual void write(processor::FactorizedTable& fTable, nodeID_t dstNodeID) const = 0;

    virtual std::unique_ptr<RJOutputWriter> copy() = 0;

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
    std::unique_ptr<function::FrontierPair> frontierPair;
    std::unique_ptr<function::EdgeCompute> edgeCompute;
    std::unique_ptr<RJOutputs> outputs;
    std::unique_ptr<RJOutputWriter> outputWriter;

    explicit RJCompState(std::unique_ptr<function::FrontierPair> frontierPair3,
        std::unique_ptr<function::EdgeCompute> edgeCompute, std::unique_ptr<RJOutputs> outputs,
        std::unique_ptr<RJOutputWriter> outputWriter);

    void initRJFromSource(nodeID_t sourceNodeID) const {
        frontierPair->initRJFromSource(sourceNodeID);
        outputs->initRJFromSource(sourceNodeID);
    }
    // When performing computations on multi-label graphs, it may be beneficial to fix a single
    // node table of nodes in the current frontier and a single node table of nodes for the next
    // frontier. That is because algorithms will perform extensions using a single relationship
    // table at a time, and each relationship table R is between a single source node table S and
    // a single destination node table T. Therefore, during execution the algorithm will need to
    // check only the active S nodes in current frontier and update the active statuses of only the
    // T nodes in the next frontier. The information that the algorithm is beginning and S-to-T
    // extensions are be given to the data structures of the computation, e.g., FrontierPairs and
    // RJOutputs, to possibly avoid them doing lookups of S and T-related data structures,
    // e.g., maps, internally.
    void beginFrontierComputeBetweenTables(table_id_t curFrontierTableID,
        table_id_t nextFrontierTableID) const {
        frontierPair->beginFrontierComputeBetweenTables(curFrontierTableID, nextFrontierTableID);
        outputs->beginFrontierComputeBetweenTables(curFrontierTableID, nextFrontierTableID);
    }
};

class SPOutputWriterDsts : public RJOutputWriter {
public:
    explicit SPOutputWriterDsts(main::ClientContext* context, RJOutputs* rjOutputs)
        : RJOutputWriter(context, rjOutputs){};

    void write(processor::FactorizedTable& fTable, nodeID_t dstNodeID) const override;

    bool skipWriting(nodeID_t dstNodeID) const override {
        return dstNodeID == rjOutputs->ptrCast<SPOutputs>()->sourceNodeID ||
               PathLengths::UNVISITED ==
                   rjOutputs->ptrCast<SPOutputs>()
                       ->pathLengths->getMaskValueFromCurFrontierFixedMask(dstNodeID.offset);
    }

    std::unique_ptr<RJOutputWriter> copy() override {
        return std::make_unique<SPOutputWriterDsts>(context, rjOutputs);
    }

protected:
    virtual void writeMoreAndAppend(processor::FactorizedTable& fTable, nodeID_t dstNodeID,
        uint16_t length) const;
};

class RJAlgorithm : public GDSAlgorithm {
protected:
    static constexpr char LENGTH_COLUMN_NAME[] = "length";
    static constexpr char PATH_NODE_IDS_COLUMN_NAME[] = "pathNodeIDs";

public:
    RJAlgorithm() = default;
    RJAlgorithm(const RJAlgorithm& other) : GDSAlgorithm{other} {}

    void exec(processor::ExecutionContext* executionContext) override;
    virtual RJCompState getRJCompState(processor::ExecutionContext* executionContext,
        nodeID_t sourceNodeID) = 0;

protected:
    void validateLowerUpperBound(int64_t lowerBound, int64_t upperBound);

    binder::expression_vector getNodeIDResultColumns() const;
    std::shared_ptr<binder::Expression> getLengthColumn(binder::Binder* binder) const;
    std::shared_ptr<binder::Expression> getPathNodeIDsColumn(binder::Binder* binder) const;
};

class SPAlgorithm : public RJAlgorithm {
public:
    SPAlgorithm() = default;
    SPAlgorithm(const SPAlgorithm& other) : RJAlgorithm{other} {}

    /*
     * Inputs include the following:
     *
     * graph::ANY
     * srcNode::NODE
     * upperBound::INT64
     * outputProperty::BOOL
     */
    std::vector<LogicalTypeID> getParameterTypeIDs() const override {
        return {LogicalTypeID::ANY, LogicalTypeID::NODE, LogicalTypeID::INT64, LogicalTypeID::BOOL};
    }

    void bind(const binder::expression_vector& params, binder::Binder* binder,
        graph::GraphEntry& graphEntry) override;
};

} // namespace function
} // namespace kuzu
