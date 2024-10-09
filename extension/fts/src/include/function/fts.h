#pragma once

#include "binder/expression/expression_util.h"
#include "binder/expression/node_expression.h"
#include "function/gds/gds.h"
#include "function/gds/gds_frontier.h"
#include "function/gds_function.h"

namespace kuzu {
namespace fts_extension {

struct FTSBindData final : public function::GDSBindData {
    std::shared_ptr<binder::Expression> nodeInput;
    double_t k;
    double_t b;
    uint64_t numDocs;
    double_t avgDL;

    FTSBindData(std::shared_ptr<binder::Expression> nodeInput,
        std::shared_ptr<binder::Expression> nodeOutput, double_t k, double_t b, uint64_t numDocs,
        double_t avgDL)
        : GDSBindData{std::move(nodeOutput)}, nodeInput{std::move(nodeInput)}, k{k}, b{b},
          numDocs{numDocs}, avgDL{avgDL} {}
    FTSBindData(const FTSBindData& other)
        : GDSBindData{other}, nodeInput{other.nodeInput}, k{other.k}, b{other.b},
          numDocs{other.numDocs}, avgDL{other.avgDL} {}

    bool hasNodeInput() const override { return true; }
    std::shared_ptr<binder::Expression> getNodeInput() const override { return nodeInput; }

    std::unique_ptr<GDSBindData> copy() const override {
        return std::make_unique<FTSBindData>(*this);
    }
};

// Wrapper around the data that needs to be stored during the computation of a recursive joins
// computation from one source. Also contains several initialization functions.
struct FTSState {
    std::unique_ptr<function::FrontierPair> frontierPair;
    std::unique_ptr<function::EdgeCompute> edgeCompute;

    void initFTSFromSource(common::nodeID_t sourceNodeID) const {
        frontierPair->initRJFromSource(sourceNodeID);
    }
};

class FTSAlgorithm : public function::GDSAlgorithm {
public:
    static constexpr char SCORE_COLUMN_NAME[] = "score";
    FTSAlgorithm() = default;
    FTSAlgorithm(const FTSAlgorithm& other) : GDSAlgorithm{other} {}

    /*
     * Inputs include the following:
     *
     * graph::ANY
     * srcNode::NODE
     * queryString: STRING
     */
    std::vector<common::LogicalTypeID> getParameterTypeIDs() const override {
        return {common::LogicalTypeID::ANY, common::LogicalTypeID::NODE,
            common::LogicalTypeID::DOUBLE, common::LogicalTypeID::DOUBLE,
            common::LogicalTypeID::UINT64, common::LogicalTypeID::DOUBLE};
    }

    void exec(processor::ExecutionContext* executionContext) override;

    std::unique_ptr<GDSAlgorithm> copy() const override {
        return std::make_unique<FTSAlgorithm>(*this);
    }

    binder::expression_vector getResultColumns(binder::Binder* binder) const override;

    void bind(const binder::expression_vector& params, binder::Binder* binder,
        graph::GraphEntry& graphEntry) override;
};

struct FTSFunction {
    static constexpr const char* name = "FTS";

    static function::function_set getFunctionSet();
};

} // namespace fts_extension
} // namespace kuzu
