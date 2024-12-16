#pragma once

#include "math.h"

#include "binder/expression/expression_util.h"
#include "binder/expression/node_expression.h"
#include "function/gds/gds.h"
#include "function/gds/gds_frontier.h"
#include "function/gds_function.h"

namespace kuzu {
namespace fts_extension {

class QFTSAlgorithm : public function::GDSAlgorithm {
public:
    static constexpr char SCORE_PROP_NAME[] = "score";
    static constexpr char TERM_FREQUENCY_PROP_NAME[] = "tf";
    static constexpr char DOC_LEN_PROP_NAME[] = "len";

public:
    QFTSAlgorithm() = default;
    QFTSAlgorithm(const QFTSAlgorithm& other) : GDSAlgorithm{other} {}

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
            common::LogicalTypeID::UINT64, common::LogicalTypeID::DOUBLE,
            common::LogicalTypeID::INT64, common::LogicalTypeID::BOOL, common::LogicalTypeID::STRING};
    }

    void exec(processor::ExecutionContext* executionContext) override;

    std::unique_ptr<GDSAlgorithm> copy() const override {
        return std::make_unique<QFTSAlgorithm>(*this);
    }

    binder::expression_vector getResultColumns(binder::Binder* binder) const override;

    void bind(const binder::expression_vector& params, binder::Binder* binder,
        graph::GraphEntry& graphEntry) override;
};

struct QFTSFunction {
    static constexpr const char* name = "QFTS";

    static function::function_set getFunctionSet();
};

} // namespace fts_extension
} // namespace kuzu
