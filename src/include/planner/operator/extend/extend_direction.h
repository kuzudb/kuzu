#pragma once

#include <cstdint>

#include "binder/expression/rel_expression.h"
#include "common/enums/rel_direction.h"

namespace kuzu {
namespace planner {

enum class ExtendDirection : uint8_t { FWD = 0, BWD = 1, BOTH = 2 };

struct ExtendDirectionUtils {
    static inline ExtendDirection getExtendDirection(
        const binder::RelExpression& relExpression, const binder::NodeExpression& boundNode) {
        if (relExpression.getDirectionType() == binder::RelDirectionType::BOTH) {
            return ExtendDirection::BOTH;
        }
        if (relExpression.getSrcNodeName() == boundNode.getUniqueName()) {
            return ExtendDirection::FWD;
        } else {
            return ExtendDirection::BWD;
        }
    }

    static inline common::RelDataDirection getRelDataDirection(ExtendDirection extendDirection) {
        KU_ASSERT(extendDirection != ExtendDirection::BOTH);
        return extendDirection == ExtendDirection::FWD ? common::RelDataDirection::FWD :
                                                         common::RelDataDirection::BWD;
    }
};

} // namespace planner
} // namespace kuzu
