#pragma once

#include "common/types/types.h"

namespace kuzu {
namespace common {

// Note that this class is NOT thread-safe.
class SemiMask {
public:
    explicit SemiMask(offset_t maxOffset) : maxOffset{maxOffset}, enabled{false} {}

    virtual ~SemiMask() = default;

    virtual void mask(offset_t nodeOffset) = 0;
    virtual void maskRange(offset_t startNodeOffset, offset_t endNodeOffset) = 0;

    virtual bool isMasked(offset_t startNodeOffset) = 0;

    // include&exclude
    virtual offset_vec_t range(uint32_t start, uint32_t end) = 0;

    virtual uint64_t getNumMaskedNodes() const = 0;

    offset_t getMaxOffset() const { return maxOffset; }

    bool isEnabled() const { return enabled; }
    void enable() { enabled = true; }

private:
    offset_t maxOffset;
    bool enabled;
};

struct SemiMaskUtil {
    static std::unique_ptr<SemiMask> createMask(offset_t maxOffset);
};

} // namespace common
} // namespace kuzu
