#pragma once

#include "common/type_utils.h"
#include "common/types/date_t.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {
namespace operation {

struct Century {
    static inline void operation(timestamp_t& timestamp, int64_t& result) {
        result = Timestamp::getTimestampPart(DatePartSpecifier::CENTURY, timestamp);
    }
};

struct EpochMs {
    static inline void operation(int64_t& ms, timestamp_t& result) {
        result = Timestamp::FromEpochMs(ms);
    }
};

struct ToTimestamp {
    static inline void operation(int64_t& sec, timestamp_t& result) {
        result = Timestamp::FromEpochSec(sec);
    }
};

} // namespace operation
} // namespace function
} // namespace kuzu
