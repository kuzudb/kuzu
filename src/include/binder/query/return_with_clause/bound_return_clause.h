#pragma once

#include "binder/bound_statement_result.h"
#include "bound_projection_body.h"

namespace kuzu {
namespace binder {

class BoundReturnClause {
public:
    explicit BoundReturnClause(std::unique_ptr<BoundProjectionBody> projectionBody)
        : projectionBody{std::move(projectionBody)}, statementResult{nullptr} {}
    BoundReturnClause(std::unique_ptr<BoundProjectionBody> projectionBody,
        std::unique_ptr<BoundStatementResult> statementResult)
        : projectionBody{std::move(projectionBody)}, statementResult{std::move(statementResult)} {}
    virtual ~BoundReturnClause() = default;

    inline BoundProjectionBody* getProjectionBody() { return projectionBody.get(); }

    inline BoundStatementResult* getStatementResult() const { return statementResult.get(); }

protected:
    std::unique_ptr<BoundProjectionBody> projectionBody;
    std::unique_ptr<BoundStatementResult> statementResult;
};

} // namespace binder
} // namespace kuzu
