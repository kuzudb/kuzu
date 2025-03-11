#pragma once

#include "bfs_graph.h"
#include "common/counter.h"
#include "common/enums/path_semantic.h"
#include "common/types/types.h"
#include "processor/result/factorized_table.h"

namespace kuzu {
namespace function {

class RJOutputWriter {
public:
    RJOutputWriter(main::ClientContext* context, common::NodeOffsetMaskMap* outputNodeMask,
        common::nodeID_t sourceNodeID);
    virtual ~RJOutputWriter() = default;

    void beginWritingOutputs(common::table_id_t tableID);

    bool skip(common::nodeID_t dstNodeID) const;

    virtual void write(processor::FactorizedTable& fTable, common::nodeID_t dstNodeID,
        common::LimitCounter* counter) = 0;

    virtual std::unique_ptr<RJOutputWriter> copy() = 0;

protected:
    virtual void beginWritingOutputsInternal(common::table_id_t tableID) = 0;
    virtual bool skipInternal(common::nodeID_t dstNodeID) const = 0;
    std::unique_ptr<common::ValueVector> createVector(const common::LogicalType& type);

protected:
    main::ClientContext* context;
    common::NodeOffsetMaskMap* outputNodeMask;
    common::nodeID_t sourceNodeID;

    std::vector<common::ValueVector*> vectors;
    std::unique_ptr<common::ValueVector> srcNodeIDVector;
    std::unique_ptr<common::ValueVector> dstNodeIDVector;
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
    common::NodeOffsetMaskMap* pathNodeMask = nullptr;

    bool hasNodeMask() const { return pathNodeMask != nullptr; }
};

class PathsOutputWriter : public RJOutputWriter {
public:
    PathsOutputWriter(main::ClientContext* context, common::NodeOffsetMaskMap* outputNodeMask,
        common::nodeID_t sourceNodeID, PathsOutputWriterInfo info, BFSGraph& bfsGraph);

    void beginWritingOutputsInternal(common::table_id_t tableID) override {
        bfsGraph.pinTableID(tableID);
    }

    void write(processor::FactorizedTable& fTable, common::nodeID_t dstNodeID,
        common::LimitCounter* counter) override;

protected:
    // Fast path when there is no node predicate or semantic check
    void dfsFast(ParentList* firstParent, processor::FactorizedTable& fTable,
        common::LimitCounter* counter);
    void dfsSlow(ParentList* firstParent, processor::FactorizedTable& fTable,
        common::LimitCounter* counter);

    bool updateCounterAndTerminate(common::LimitCounter* counter);

    ParentList* findFirstParent(common::offset_t dstOffset) const;

    bool checkPathNodeMask(ParentList* element) const;
    // Check semantics
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
    BFSGraph& bfsGraph;

    std::unique_ptr<common::ValueVector> directionVector = nullptr;
    std::unique_ptr<common::ValueVector> lengthVector = nullptr;
    std::unique_ptr<common::ValueVector> pathNodeIDsVector = nullptr;
    std::unique_ptr<common::ValueVector> pathEdgeIDsVector = nullptr;
};

class SPPathsOutputWriter : public PathsOutputWriter {
public:
    SPPathsOutputWriter(main::ClientContext* context, common::NodeOffsetMaskMap* outputNodeMask,
        common::nodeID_t sourceNodeID, PathsOutputWriterInfo info, BFSGraph& bfsGraph)
        : PathsOutputWriter{context, outputNodeMask, sourceNodeID, info, bfsGraph} {}

    std::unique_ptr<RJOutputWriter> copy() override {
        return std::make_unique<SPPathsOutputWriter>(context, outputNodeMask, sourceNodeID, info,
            bfsGraph);
    }

protected:
    bool skipInternal(common::nodeID_t dstNodeID) const override {
        // For single/all shortest path computations, we do not output any results from source to
        // source. We also do not output any results if a destination node has not been reached.
        return dstNodeID == sourceNodeID || nullptr == bfsGraph.getParentListHead(dstNodeID.offset);
    }
};

} // namespace function
} // namespace kuzu
