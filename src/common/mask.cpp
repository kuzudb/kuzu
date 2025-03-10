#include "common/mask.h"

namespace kuzu {
namespace common {

offset_vec_t Roaring32BitmapSemiMask::range(uint32_t start, uint32_t end) {
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
}

offset_vec_t Roaring64BitmapSemiMask::range(uint32_t start, uint32_t end) {
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
}

std::unique_ptr<RoaringBitmapSemiMask> RoaringBitmapSemiMaskUtil::createMask(offset_t maxOffset) {
    if (maxOffset > std::numeric_limits<uint32_t>::max()) {
        return std::make_unique<Roaring64BitmapSemiMask>(maxOffset);
    } else {
        return std::make_unique<Roaring32BitmapSemiMask>(maxOffset);
    }
}

offset_t NodeOffsetMaskMap::getNumMaskedNode() const {
    offset_t numNodes = 0;
    for (auto& [tableID, mask] : maskMap) {
        numNodes += mask->getNumMaskedNodes();
    }
    return numNodes;
}

} // namespace common
} // namespace kuzu
