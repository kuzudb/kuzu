#pragma once

#include <memory>

#include "common/types/types.h"
#include "roaring.hh"

namespace kuzu {
namespace common {

class RoaringBitmapSemiMask;

using semi_mask_t = RoaringBitmapSemiMask;

// unsafe
class RoaringBitmapSemiMask {
public:
    explicit RoaringBitmapSemiMask(common::offset_t maxOffset)
        : maxOffset{maxOffset}, enabled{false} {}

    virtual ~RoaringBitmapSemiMask() = default;

    virtual void mask(common::offset_t nodeOffset) = 0;
    virtual void maskRange(common::offset_t startNodeOffset, common::offset_t endNodeOffset) = 0;

    virtual bool isMasked(common::offset_t startNodeOffset) = 0;

    // include&exclude
    virtual offset_vec_t range(uint32_t start, uint32_t end) = 0;

    virtual uint64_t getNumMaskedNodes() const = 0;

    common::offset_t getMaxOffset() const { return maxOffset; }

    bool isEnabled() const { return enabled; }
    void enable() { enabled = true; }

private:
    common::offset_t maxOffset;
    bool enabled;
};

class Roaring32BitmapSemiMask : public RoaringBitmapSemiMask {
public:
    explicit Roaring32BitmapSemiMask(common::offset_t maxOffset)
        : RoaringBitmapSemiMask(maxOffset), roaring(std::make_shared<roaring::Roaring>()) {}

    void mask(common::offset_t nodeOffset) override { roaring->add(nodeOffset); }
    void maskRange(common::offset_t startNodeOffset, common::offset_t endNodeOffset) override {
        roaring->addRange(startNodeOffset, endNodeOffset);
    }

    bool isMasked(common::offset_t startNodeOffset) override {
        return roaring->contains(startNodeOffset);
    }

    uint64_t getNumMaskedNodes() const override { return roaring->cardinality(); }

    // include&exclude
    offset_vec_t range(uint32_t start, uint32_t end) override {
        auto it = roaring->begin();
        it.equalorlarger(start);
        offset_vec_t ans;
        for (; it != roaring->end(); it++) {
            auto value = *it;
            if (value >= end) {
                break;
            }
            ans.push_back(value);
        }
        return ans;
    };

    std::shared_ptr<roaring::Roaring> roaring;
};

class Roaring64BitmapSemiMask : public RoaringBitmapSemiMask {
public:
    explicit Roaring64BitmapSemiMask(common::offset_t maxOffset)
        : RoaringBitmapSemiMask(maxOffset), roaring(std::make_shared<roaring::Roaring64Map>()) {}

    void mask(common::offset_t nodeOffset) override { roaring->add(nodeOffset); }
    void maskRange(common::offset_t startNodeOffset, common::offset_t endNodeOffset) override {
        roaring->addRange(startNodeOffset, endNodeOffset);
    }

    bool isMasked(common::offset_t startNodeOffset) override {
        return roaring->contains(startNodeOffset);
    }

    uint64_t getNumMaskedNodes() const override { return roaring->cardinality(); }

    // include&exclude
    offset_vec_t range(uint32_t start, uint32_t end) override {
        auto it = roaring->begin();
        it.move(start);
        offset_vec_t ans;
        for (; it != roaring->end(); it++) {
            auto value = *it;
            if (value >= end) {
                break;
            }
            ans.push_back(value);
        }
        return ans;
    };

    std::shared_ptr<roaring::Roaring64Map> roaring;
};

struct RoaringBitmapSemiMaskUtil {
    static std::unique_ptr<RoaringBitmapSemiMask> createMask(common::offset_t maxOffset) {
        if (maxOffset > std::numeric_limits<uint32_t>::max()) {
            return std::make_unique<Roaring64BitmapSemiMask>(maxOffset);
        } else {
            return std::make_unique<Roaring32BitmapSemiMask>(maxOffset);
        }
    }
};

} // namespace common
} // namespace kuzu
