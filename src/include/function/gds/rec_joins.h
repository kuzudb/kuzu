#pragma once

#include "common/enums/extend_direction.h"
#include "common/enums/path_semantic.h"
#include "function/gds/gds.h"
#include "function/gds/gds_frontier.h"
#include "function/gds/gds_utils.h"
#include "output_writer.h"

namespace kuzu {
namespace function {

struct RJBindData : public GDSBindData {
    static constexpr uint16_t DEFAULT_MAXIMUM_ALLOWED_UPPER_BOUND = (uint16_t)255;

    std::shared_ptr<binder::Expression> nodeInput = nullptr;
    // For any form of shortest path lower bound must always be 1.
    // If lowerBound equals to 0, an empty path with source node only will be returned.
    uint16_t lowerBound = 0;
    uint16_t upperBound = 0;
    common::PathSemantic semantic = common::PathSemantic::WALK;

    common::ExtendDirection extendDirection = common::ExtendDirection::FWD;

    bool flipPath = false; // See PathsOutputWriterInfo::flipPath for comments.
    bool writePath = true;

    std::shared_ptr<binder::Expression> directionExpr = nullptr;
    std::shared_ptr<binder::Expression> lengthExpr = nullptr;
    std::shared_ptr<binder::Expression> pathNodeIDsExpr = nullptr;
    std::shared_ptr<binder::Expression> pathEdgeIDsExpr = nullptr;

    std::string weightPropertyName;
    std::shared_ptr<binder::Expression> weightOutputExpr = nullptr;

    RJBindData(graph::GraphEntry graphEntry, std::shared_ptr<binder::Expression> nodeOutput)
        : GDSBindData{std::move(graphEntry), std::move(nodeOutput)} {}
    RJBindData(const RJBindData& other);

    bool hasNodeInput() const override { return true; }
    std::shared_ptr<binder::Expression> getNodeInput() const override { return nodeInput; }
    bool hasNodeOutput() const override { return true; }

    PathsOutputWriterInfo getPathWriterInfo() const;

    std::unique_ptr<GDSBindData> copy() const override {
        return std::make_unique<RJBindData>(*this);
    }
};

// Wrapper around the data that needs to be stored during the computation of a recursive joins
// computation from one source. Also contains several initialization functions.
struct RJCompState : public GDSComputeState {
    std::unique_ptr<RJOutputs> outputs;
    std::unique_ptr<RJOutputWriter> outputWriter;

    RJCompState(std::unique_ptr<function::FrontierPair> frontierPair,
        std::unique_ptr<function::EdgeCompute> edgeCompute,
        processor::NodeOffsetMaskMap* outputNodeMask, std::unique_ptr<RJOutputs> outputs,
        std::unique_ptr<RJOutputWriter> outputWriter)
        : GDSComputeState{std::move(frontierPair), std::move(edgeCompute), outputNodeMask},
          outputs{std::move(outputs)}, outputWriter{std::move(outputWriter)} {}

    void initSource(common::nodeID_t sourceNodeID) const {
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
    void beginFrontierComputeBetweenTables(common::table_id_t currTableID,
        common::table_id_t nextTableID) override {
        GDSComputeState::beginFrontierComputeBetweenTables(currTableID, nextTableID);
        outputs->beginFrontierComputeBetweenTables(currTableID, nextTableID);
    }
};

class RJAlgorithm : public GDSAlgorithm {
    static constexpr char DIRECTION_COLUMN_NAME[] = "direction";
    static constexpr char LENGTH_COLUMN_NAME[] = "length";
    static constexpr char PATH_NODE_IDS_COLUMN_NAME[] = "pathNodeIDs";
    static constexpr char PATH_EDGE_IDS_COLUMN_NAME[] = "pathEdgeIDs";

public:
    RJAlgorithm() = default;
    RJAlgorithm(const RJAlgorithm& other) : GDSAlgorithm{other} {}

    void exec(processor::ExecutionContext* context) override;
    virtual RJCompState getRJCompState(processor::ExecutionContext* context,
        common::nodeID_t sourceNodeID) = 0;
    void setToNoPath();
    binder::expression_vector getResultColumnsNoPath();

protected:
    void validateLowerUpperBound(int64_t lowerBound, int64_t upperBound);

    binder::expression_vector getBaseResultColumns() const;
    void bindColumnExpressions(binder::Binder* binder) const;

    std::unique_ptr<BFSGraph> getBFSGraph(processor::ExecutionContext* context);
};

class SPAlgorithm : public RJAlgorithm {
public:
    SPAlgorithm() = default;
    SPAlgorithm(const SPAlgorithm& other) : RJAlgorithm{other} {}

    // Inputs are graph, srcNode, upperBound, direction
    std::vector<common::LogicalTypeID> getParameterTypeIDs() const override {
        return {common::LogicalTypeID::ANY, common::LogicalTypeID::NODE,
            common::LogicalTypeID::INT64, common::LogicalTypeID::STRING};
    }

    void bind(const GDSBindInput& input, main::ClientContext&) override;
};

} // namespace function
} // namespace kuzu
