#pragma once

#include "projection_body.h"

namespace kuzu {
namespace parser {

class ReturnClause {

public:
    explicit ReturnClause(std::unique_ptr<ProjectionBody> projectionBody)
        : projectionBody{std::move(projectionBody)} {}

    virtual ~ReturnClause() = default;

    inline ProjectionBody* getProjectionBody() const { return projectionBody.get(); }

private:
    std::unique_ptr<ProjectionBody> projectionBody;
};

} // namespace parser
} // namespace kuzu
