#pragma once

#include "math.h"

#include "binder/expression/expression_util.h"
#include "binder/expression/node_expression.h"
#include "function/gds/gds.h"
#include "function/gds/gds_frontier.h"
#include "function/gds_function.h"

namespace kuzu {
namespace fts_extension {

class FTSAlgorithm : public function::GDSAlgorithm {
public:
    static constexpr char SCORE_PROP_NAME[] = "score";
    static constexpr char TERM_FREQUENCY_PROP_NAME[] = "tf";
    static constexpr char DOC_LEN_PROP_NAME[] = "len";
    static constexpr uint64_t NUM_THREADS_FOR_EXECUTION = 1;

public:
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
