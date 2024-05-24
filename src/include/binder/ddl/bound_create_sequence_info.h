#pragma once

#include <string>

#include "common/copy_constructors.h"

namespace kuzu {
namespace binder {

struct BoundCreateSequenceInfo {
    std::string sequenceName;
    int64_t startWith;
    int64_t increment;
    int64_t minValue;
    int64_t maxValue;
    bool cycle;

    BoundCreateSequenceInfo(std::string sequenceName, int64_t startWith, int64_t increment,
        int64_t minValue, int64_t maxValue, bool cycle)
        : sequenceName{sequenceName}, startWith{startWith}, increment{increment},
          minValue{minValue}, maxValue{maxValue}, cycle{cycle} {}
    EXPLICIT_COPY_DEFAULT_MOVE(BoundCreateSequenceInfo);

private:
    BoundCreateSequenceInfo(const BoundCreateSequenceInfo& other)
        : sequenceName{other.sequenceName}, startWith{other.startWith}, increment{other.increment},
          minValue{other.minValue}, maxValue{other.maxValue}, cycle{other.cycle} {}
};

} // namespace binder
} // namespace kuzu
