#pragma once

#include <string>

#include "common/copy_constructors.h"
#include "common/enums/conflict_action.h"

namespace kuzu {
namespace binder {

struct BoundCreateSequenceInfo {
    std::string sequenceName;
    int64_t startWith;
    int64_t increment;
    int64_t minValue;
    int64_t maxValue;
    bool cycle;
    common::ConflictAction onConflict;
    bool hasParent = false;

    BoundCreateSequenceInfo(std::string sequenceName, int64_t startWith, int64_t increment,
        int64_t minValue, int64_t maxValue, bool cycle, common::ConflictAction onConflict)
        : sequenceName{sequenceName}, startWith{startWith}, increment{increment},
          minValue{minValue}, maxValue{maxValue}, cycle{cycle}, onConflict{onConflict} {}
    EXPLICIT_COPY_DEFAULT_MOVE(BoundCreateSequenceInfo);

private:
    BoundCreateSequenceInfo(const BoundCreateSequenceInfo& other)
        : sequenceName{other.sequenceName}, startWith{other.startWith}, increment{other.increment},
          minValue{other.minValue}, maxValue{other.maxValue}, cycle{other.cycle},
          onConflict{other.onConflict}, hasParent{other.hasParent} {}
};

} // namespace binder
} // namespace kuzu
