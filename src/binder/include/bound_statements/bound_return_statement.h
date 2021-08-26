#pragma once

#include "src/binder/include/bound_statements/bound_projection_body.h"

namespace graphflow {
namespace binder {

class BoundReturnStatement {

public:
    explicit BoundReturnStatement(unique_ptr<BoundProjectionBody> projectionBody)
        : projectionBody{move(projectionBody)} {}

    inline BoundProjectionBody* getBoundProjectionBody() const { return projectionBody.get(); }

    virtual vector<shared_ptr<Expression>> getDependentProperties() const {
        return projectionBody->getDependentProperties();
    }

private:
    unique_ptr<BoundProjectionBody> projectionBody;
};

} // namespace binder
} // namespace graphflow
