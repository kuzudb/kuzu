#pragma once

#include "common/enums/extend_direction.h"
#include "common/enums/path_semantic.h"
#include "function/gds/gds.h"
#include "function/gds/gds_frontier.h"
#include "output_writer.h"

namespace kuzu {
namespace function {

struct RJBindData final : public GDSBindData {
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
    common::PathSemantic semantic;

    common::ExtendDirection extendDirection = common::ExtendDirection::FWD;

    bool extendFromSource = true;
    bool writePath = true;

    std::shared_ptr<binder::Expression> directionExpr = nullptr;
    std::shared_ptr<binder::Expression> lengthExpr = nullptr;
    std::shared_ptr<binder::Expression> pathNodeIDsExpr = nullptr;
    std::shared_ptr<binder::Expression> pathEdgeIDsExpr = nullptr;

    RJBindData(std::shared_ptr<binder::Expression> nodeInput,
        std::shared_ptr<binder::Expression> nodeOutput, uint16_t lowerBound, uint16_t upperBound,
        common::PathSemantic semantic, common::ExtendDirection extendDirection)
        : GDSBindData{std::move(nodeOutput)}, nodeInput{std::move(nodeInput)},
          lowerBound{lowerBound}, upperBound{upperBound}, semantic{semantic},
          extendDirection{extendDirection} {
        KU_ASSERT(upperBound < DEFAULT_MAXIMUM_ALLOWED_UPPER_BOUND);
    }
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
struct RJCompState {
    std::unique_ptr<function::FrontierPair> frontierPair;
    std::unique_ptr<function::EdgeCompute> edgeCompute;
    std::unique_ptr<RJOutputs> outputs;
    std::unique_ptr<RJOutputWriter> outputWriter;

    RJCompState(std::unique_ptr<function::FrontierPair> frontierPair,
        std::unique_ptr<function::EdgeCompute> edgeCompute, std::unique_ptr<RJOutputs> outputs,
        std::unique_ptr<RJOutputWriter> outputWriter)
        : frontierPair{std::move(frontierPair)}, edgeCompute{std::move(edgeCompute)},
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
    void beginFrontierComputeBetweenTables(common::table_id_t curFrontierTableID,
        common::table_id_t nextFrontierTableID) const {
        frontierPair->beginFrontierComputeBetweenTables(curFrontierTableID, nextFrontierTableID);
        outputs->beginFrontierComputeBetweenTables(curFrontierTableID, nextFrontierTableID);
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

    void exec(processor::ExecutionContext* executionContext) override;
    virtual RJCompState getRJCompState(processor::ExecutionContext* executionContext,
        common::nodeID_t sourceNodeID) = 0;
    void setToNoPath();
    binder::expression_vector getResultColumnsNoPath();

protected:
    void validateLowerUpperBound(int64_t lowerBound, int64_t upperBound);

    binder::expression_vector getBaseResultColumns() const;
    void bindColumnExpressions(binder::Binder* binder) const;
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
     * direction::STRING
     */
    std::vector<common::LogicalTypeID> getParameterTypeIDs() const override {
        return {common::LogicalTypeID::ANY, common::LogicalTypeID::NODE,
            common::LogicalTypeID::INT64, common::LogicalTypeID::STRING};
    }

    void bind(const binder::expression_vector& params, binder::Binder* binder,
        graph::GraphEntry& graphEntry) override;
};

} // namespace function
} // namespace kuzu
