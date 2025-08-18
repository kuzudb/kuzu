#pragma once

#include <cstdint>

namespace kuzu {
namespace common {

struct ArrowResultConfig {
    int64_t chunkSize;

    ArrowResultConfig() : chunkSize(1000) {}
    explicit ArrowResultConfig(int64_t chunkSize) : chunkSize(chunkSize) {}
};

}
}
