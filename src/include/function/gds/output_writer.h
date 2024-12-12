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
    SPOutputs(common::nodeID_t sourceNodeID, std::shared_ptr<PathLengths> pathLengths)
        : RJOutputs{sourceNodeID}, pathLengths{std::move(pathLengths)} {}

public:
    std::shared_ptr<PathLengths> pathLengths;
};

struct PathsOutputs : public SPOutputs {
    PathsOutputs(common::nodeID_t sourceNodeID, std::shared_ptr<PathLengths> pathLengths,
        std::unique_ptr<BFSGraph> bfsGraph)
        : SPOutputs(sourceNodeID, std::move(pathLengths)), bfsGraph{std::move(bfsGraph)} {}

    void beginFrontierComputeBetweenTables(common::table_id_t,
        common::table_id_t nextFrontierTableID) override {
        // Note: We do not fix the node table for pathLengths, which is inherited from AllSPOutputs.
        // See the comment in SingleSPOutputs::beginFrontierComputeBetweenTables() for details.
        bfsGraph->pinTableID(nextFrontierTableID);
    };

    void beginWritingOutputsForDstNodesInTable(common::table_id_t tableID) override;

public:
    std::unique_ptr<BFSGraph> bfsGraph;
};

class GDSOutputWriter {
public:
    GDSOutputWriter(main::ClientContext* context, processor::NodeOffsetMaskMap* outputNodeMask)
        : context{context}, outputNodeMask{outputNodeMask} {}
    virtual ~GDSOutputWriter() = default;

    virtual void pinTableID(common::table_id_t tableID) {
        if (outputNodeMask != nullptr) {
            outputNodeMask->pin(tableID);
        }
    }

    processor::NodeOffsetMaskMap* getOutputNodeMask() const { return outputNodeMask; }

protected:
    std::unique_ptr<common::ValueVector> createVector(const common::LogicalType& type,
        storage::MemoryManager* mm);

protected:
    main::ClientContext* context;
    processor::NodeOffsetMaskMap* outputNodeMask;
    std::vector<common::ValueVector*> vectors;
};

class RJOutputWriter : public GDSOutputWriter {
public:
    RJOutputWriter(main::ClientContext* context, RJOutputs* rjOutputs,
        processor::NodeOffsetMaskMap* outputNodeMask);

    void pinTableID(common::table_id_t tableID) override;

    bool skip(common::nodeID_t dstNodeID) const;

    virtual void write(processor::FactorizedTable& fTable, common::nodeID_t dstNodeID,
        processor::GDSOutputCounter* counter) = 0;

    virtual std::unique_ptr<RJOutputWriter> copy() = 0;

protected:
    virtual bool skipInternal(common::nodeID_t dstNodeID) const = 0;

protected:
    RJOutputs* rjOutputs;
    std::unique_ptr<common::ValueVector> srcNodeIDVector;
    std::unique_ptr<common::ValueVector> dstNodeIDVector;
};

class DestinationsOutputWriter : public RJOutputWriter {
public:
    DestinationsOutputWriter(main::ClientContext* context, RJOutputs* rjOutputs,
        processor::NodeOffsetMaskMap* outputNodeMask);

    void write(processor::FactorizedTable& fTable, common::nodeID_t dstNodeID,
        processor::GDSOutputCounter* counter) override;

    std::unique_ptr<RJOutputWriter> copy() override {
        return std::make_unique<DestinationsOutputWriter>(context, rjOutputs, outputNodeMask);
    }

protected:
    bool skipInternal(common::nodeID_t dstNodeID) const override;

protected:
    std::unique_ptr<common::ValueVector> lengthVector;
};

struct PathsOutputWriterInfo {
    // Semantic
    common::PathSemantic semantic = common::PathSemantic::WALK;
    // Range
    uint16_t lowerBound = 0;
    // Direction
    bool flipPath = false;
    bool writeEdgeDirection = false;
    bool writePath = false;
    // Node predicate mask
    processor::NodeOffsetMaskMap* pathNodeMask = nullptr;

    bool hasNodeMask() const { return pathNodeMask != nullptr; }
};

class PathsOutputWriter : public RJOutputWriter {
public:
    PathsOutputWriter(main::ClientContext* context, RJOutputs* rjOutputs,
        processor::NodeOffsetMaskMap* outputNodeMask, PathsOutputWriterInfo info);

    void write(processor::FactorizedTable& fTable, common::nodeID_t dstNodeID,
        processor::GDSOutputCounter* counter) override;

private:
    ParentList* findFirstParent(common::offset_t dstOffset, BFSGraph& bfsGraph) const;

    bool checkPathNodeMask(ParentList* element) const;

    bool checkAppendSemantic(const std::vector<ParentList*>& path, ParentList* candidate) const;
    bool checkReplaceTopSemantic(const std::vector<ParentList*>& path, ParentList* candidate) const;
    bool isAppendTrail(const std::vector<ParentList*>& path, ParentList* candidate) const;
    bool isAppendAcyclic(const std::vector<ParentList*>& path, ParentList* candidate) const;
    bool isReplaceTopTrail(const std::vector<ParentList*>& path, ParentList* candidate) const;
    bool isReplaceTopAcyclic(const std::vector<ParentList*>& path, ParentList* candidate) const;

    bool isNextViable(ParentList* next, const std::vector<ParentList*>& path) const;

    void beginWritePath(common::idx_t length) const;
    void writePath(const std::vector<ParentList*>& path) const;
    void writePathFwd(const std::vector<ParentList*>& path) const;
    void writePathBwd(const std::vector<ParentList*>& path) const;

    void addEdge(common::relID_t edgeID, bool fwdEdge, common::sel_t pos) const;
    void addNode(common::nodeID_t nodeID, common::sel_t pos) const;

protected:
    PathsOutputWriterInfo info;

    std::unique_ptr<common::ValueVector> directionVector = nullptr;
    std::unique_ptr<common::ValueVector> lengthVector = nullptr;
    std::unique_ptr<common::ValueVector> pathNodeIDsVector = nullptr;
    std::unique_ptr<common::ValueVector> pathEdgeIDsVector = nullptr;
};

class SPPathsOutputWriter : public PathsOutputWriter {
public:
    SPPathsOutputWriter(main::ClientContext* context, RJOutputs* rjOutputs,
        processor::NodeOffsetMaskMap* outputNodeMask, PathsOutputWriterInfo info)
        : PathsOutputWriter{context, rjOutputs, outputNodeMask, std::move(info)} {}

    std::unique_ptr<RJOutputWriter> copy() override {
        return std::make_unique<SPPathsOutputWriter>(context, rjOutputs, outputNodeMask, info);
    }

protected:
    bool skipInternal(common::nodeID_t dstNodeID) const override {
        auto pathsOutputs = rjOutputs->ptrCast<PathsOutputs>();
        // For single/all shortest path computations, we do not output any results from source to
        // source. We also do not output any results if a destination node has not been reached.
        return dstNodeID == pathsOutputs->sourceNodeID ||
               nullptr == pathsOutputs->bfsGraph->getParentListHead(dstNodeID.offset);
    }
};

} // namespace function
} // namespace kuzu
