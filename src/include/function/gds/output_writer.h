#pragma once

#include "bfs_graph.h"
#include "common/enums/path_semantic.h"
#include "common/types/types.h"
#include "processor/operator/gds_call_shared_state.h"
#include "processor/result/factorized_table.h"

namespace kuzu {
namespace function {

class PathLengths;

struct RJOutputs {
public:
    explicit RJOutputs(common::nodeID_t sourceNodeID) : sourceNodeID{sourceNodeID} {}
    virtual ~RJOutputs() = default;

    virtual void initRJFromSource(common::nodeID_t) {}
    virtual void beginFrontierComputeBetweenTables(common::table_id_t curFrontierTableID,
        common::table_id_t nextFrontierTableID) = 0;
    // This function is called after the recursive join computation stage, at the stage when the
    // outputs that are stored in RJOutputs is being written to FactorizedTable.
    virtual void beginWritingOutputsForDstNodesInTable(common::table_id_t tableID) = 0;
    template<class TARGET>
    TARGET* ptrCast() {
        return common::ku_dynamic_cast<TARGET*>(this);
    }

public:
    common::nodeID_t sourceNodeID;
};

struct SPOutputs : public RJOutputs {
public:
    SPOutputs(std::unordered_map<common::table_id_t, uint64_t> nodeTableIDAndNumNodes,
        common::nodeID_t sourceNodeID, storage::MemoryManager* mm);

public:
    std::shared_ptr<PathLengths> pathLengths;
};

struct PathsOutputs : public SPOutputs {
    PathsOutputs(std::unordered_map<common::table_id_t, uint64_t> nodeTableIDAndNumNodes,
        common::nodeID_t sourceNodeID, storage::MemoryManager* mm = nullptr)
        : SPOutputs(nodeTableIDAndNumNodes, sourceNodeID, mm),
          bfsGraph{nodeTableIDAndNumNodes, mm} {}

    void beginFrontierComputeBetweenTables(common::table_id_t,
        common::table_id_t nextFrontierTableID) override {
        // Note: We do not fix the node table for pathLengths, which is inherited from AllSPOutputs.
        // See the comment in SingleSPOutputs::beginFrontierComputeBetweenTables() for details.
        bfsGraph.pinNodeTable(nextFrontierTableID);
    };

    void beginWritingOutputsForDstNodesInTable(common::table_id_t tableID) override;

public:
    BFSGraph bfsGraph;
};

class RJOutputWriter {
public:
    explicit RJOutputWriter(main::ClientContext* context, RJOutputs* rjOutputs);
    virtual ~RJOutputWriter() = default;

    void beginWritingForDstNodesInTable(common::table_id_t tableID) {
        rjOutputs->beginWritingOutputsForDstNodesInTable(tableID);
    }

    virtual bool skipWriting(common::nodeID_t dstNodeID) const = 0;

    virtual void write(processor::FactorizedTable& fTable, common::nodeID_t dstNodeID) const = 0;

    virtual std::unique_ptr<RJOutputWriter> copy() = 0;

protected:
    main::ClientContext* context;
    RJOutputs* rjOutputs;
    std::unique_ptr<common::ValueVector> srcNodeIDVector;
    std::unique_ptr<common::ValueVector> dstNodeIDVector;
    std::vector<common::ValueVector*> vectors;
};

class DestinationsOutputWriter : public RJOutputWriter {
public:
    DestinationsOutputWriter(main::ClientContext* context, RJOutputs* rjOutputs)
        : RJOutputWriter(context, rjOutputs){};

    void write(processor::FactorizedTable& fTable, common::nodeID_t dstNodeID) const override;

    bool skipWriting(common::nodeID_t dstNodeID) const override;

    std::unique_ptr<RJOutputWriter> copy() override {
        return std::make_unique<DestinationsOutputWriter>(context, rjOutputs);
    }

protected:
    virtual void writeMoreAndAppend(processor::FactorizedTable& fTable, common::nodeID_t dstNodeID,
        uint16_t length) const;
};

struct PathsOutputWriterInfo {
    // Semantic
    common::PathSemantic semantic = common::PathSemantic::WALK;
    // Range
    uint16_t lowerBound = 0;
    // Direction
    bool extendFromSource = false;
    bool writeEdgeDirection = false;
    // Node predicate mask
    processor::NodeOffsetMaskMap* pathNodeMask = nullptr;

    bool hasNodeMask() const { return pathNodeMask != nullptr; }
};

class PathsOutputWriter : public RJOutputWriter {
public:
    PathsOutputWriter(main::ClientContext* context, RJOutputs* rjOutputs,
        PathsOutputWriterInfo info);

    void write(processor::FactorizedTable& fTable, common::nodeID_t dstNodeID) const override;

private:
    bool checkPathNodeMask(const std::vector<ParentList*>& path) const;
    bool checkSemantic(const std::vector<ParentList*>& path) const;
    bool isTrail(const std::vector<ParentList*>& path) const;
    bool isAcyclic(const std::vector<ParentList*>& path) const;

    void beginWritePath(common::idx_t length) const;
    void writePath(const std::vector<ParentList*>& path) const;
    void writePathFwd(const std::vector<ParentList*>& path) const;
    void writePathBwd(const std::vector<ParentList*>& path) const;

    void addEdge(common::relID_t edgeID, bool fwdEdge, common::sel_t pos) const;
    void addNode(common::nodeID_t nodeID, common::sel_t pos) const;

protected:
    PathsOutputWriterInfo info;

    std::unique_ptr<common::ValueVector> directionVector;
    std::unique_ptr<common::ValueVector> lengthVector;
    std::unique_ptr<common::ValueVector> pathNodeIDsVector;
    std::unique_ptr<common::ValueVector> pathEdgeIDsVector;
};

class SPPathsOutputWriter : public PathsOutputWriter {
public:
    SPPathsOutputWriter(main::ClientContext* context, RJOutputs* rjOutputs,
        PathsOutputWriterInfo info)
        : PathsOutputWriter(context, rjOutputs, std::move(info)) {}

    bool skipWriting(common::nodeID_t dstNodeID) const override {
        auto pathsOutputs = rjOutputs->ptrCast<PathsOutputs>();
        // For single/all shortest path computations, we do not output any results from source to
        // source. We also do not output any results if a destination node has not been reached.
        return dstNodeID == pathsOutputs->sourceNodeID ||
               nullptr == pathsOutputs->bfsGraph.getCurFixedParentPtrs()[dstNodeID.offset].load(
                              std::memory_order_relaxed);
    }

    std::unique_ptr<RJOutputWriter> copy() override {
        return std::make_unique<SPPathsOutputWriter>(context, rjOutputs, info);
    }
};

} // namespace function
} // namespace kuzu
