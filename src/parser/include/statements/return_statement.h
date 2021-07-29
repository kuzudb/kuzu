#pragma once

#include "src/parser/include/statements/projection_body.h"

namespace graphflow {
namespace parser {

/**
 * Return statement must have a vector of expressions to project.
 * Return statement may have limit expression.
 */
class ReturnStatement {

public:
    ReturnStatement(unique_ptr<ProjectionBody> projectionBody)
        : projectionBody{move(projectionBody)} {}

    inline ProjectionBody* getProjectionBody() const { return projectionBody.get(); }

    virtual ~ReturnStatement() = default;

private:
    unique_ptr<ProjectionBody> projectionBody;
};

} // namespace parser
} // namespace graphflow
