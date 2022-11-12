#pragma once

#include "bound_projection_body.h"

namespace kuzu {
namespace binder {

class BoundReturnClause {

public:
    explicit BoundReturnClause(unique_ptr<BoundProjectionBody> projectionBody)
        : projectionBody{move(projectionBody)} {}

    inline BoundProjectionBody* getProjectionBody() const { return projectionBody.get(); }

private:
    unique_ptr<BoundProjectionBody> projectionBody;
};

} // namespace binder
} // namespace kuzu
