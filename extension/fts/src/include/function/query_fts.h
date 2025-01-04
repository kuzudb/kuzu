#pragma once

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
    static constexpr char DOC_ID_PROP_NAME[] = "docID";

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
        return {common::LogicalTypeID::STRING /* tableName */,
            common::LogicalTypeID::STRING /* indexName */,
            common::LogicalTypeID::STRING /* query */};
    }

    void exec(processor::ExecutionContext* executionContext) override;

    std::unique_ptr<GDSAlgorithm> copy() const override {
        return std::make_unique<QFTSAlgorithm>(*this);
    }

    binder::expression_vector getResultColumns(binder::Binder* binder) const override;

    void bind(const function::GDSBindInput& input, main::ClientContext&) override;
};

struct QFTSFunction {
    static constexpr const char* name = "QUERY_FTS_INDEX";

    static function::function_set getFunctionSet();
};

} // namespace fts_extension
} // namespace kuzu
