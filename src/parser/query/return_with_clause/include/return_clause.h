#pragma once

#include "projection_body.h"

namespace graphflow {
namespace parser {

class ReturnClause {

public:
    ReturnClause(unique_ptr<ProjectionBody> projectionBody)
        : projectionBody{move(projectionBody)} {}

    virtual ~ReturnClause() = default;

    inline ProjectionBody* getProjectionBody() const { return projectionBody.get(); }

    bool operator==(const ReturnClause& other) const {
        return *projectionBody == *other.projectionBody;
    }

    bool operator!=(const ReturnClause& other) const { return !operator==(other); }

private:
    unique_ptr<ProjectionBody> projectionBody;
};

} // namespace parser
} // namespace graphflow
