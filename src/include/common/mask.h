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
    offset_vec_t range(uint32_t start, uint32_t end) override;

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
    offset_vec_t range(uint32_t start, uint32_t end) override;

    std::shared_ptr<roaring::Roaring64Map> roaring;
};

struct RoaringBitmapSemiMaskUtil {
    static std::unique_ptr<RoaringBitmapSemiMask> createMask(common::offset_t maxOffset);
};

class NodeOffsetMaskMap {
public:
    NodeOffsetMaskMap() : enabled_{false} {}

    void enable() { enabled_ = true; }
    bool enabled() const { return enabled_; }

    common::offset_t getNumMaskedNode() const;

    void addMask(common::table_id_t tableID, std::unique_ptr<common::semi_mask_t> mask) {
        KU_ASSERT(!maskMap.contains(tableID));
        maskMap.insert({tableID, std::move(mask)});
    }

    common::table_id_map_t<common::semi_mask_t*> getMasks() const {
        common::table_id_map_t<common::semi_mask_t*> result;
        for (auto& [tableID, mask] : maskMap) {
            result.emplace(tableID, mask.get());
        }
        return result;
    }

    bool containsTableID(common::table_id_t tableID) const { return maskMap.contains(tableID); }
    common::semi_mask_t* getOffsetMask(common::table_id_t tableID) const {
        KU_ASSERT(containsTableID(tableID));
        return maskMap.at(tableID).get();
    }

    void pin(common::table_id_t tableID) {
        if (maskMap.contains(tableID)) {
            pinnedMask = maskMap.at(tableID).get();
        } else {
            pinnedMask = nullptr;
        }
    }
    bool hasPinnedMask() const { return pinnedMask != nullptr; }
    common::semi_mask_t* getPinnedMask() const { return pinnedMask; }

    bool valid(common::offset_t offset) {
        KU_ASSERT(pinnedMask != nullptr);
        return pinnedMask->isMasked(offset);
    }
    bool valid(common::nodeID_t nodeID) {
        KU_ASSERT(maskMap.contains(nodeID.tableID));
        return maskMap.at(nodeID.tableID)->isMasked(nodeID.offset);
    }

private:
    common::table_id_map_t<std::unique_ptr<common::semi_mask_t>> maskMap;
    common::semi_mask_t* pinnedMask = nullptr;
    // If mask map is enabled, then some nodes might be masked.
    bool enabled_;
};

} // namespace common
} // namespace kuzu
