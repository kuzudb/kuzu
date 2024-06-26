#pragma once

#include "processor/operator/mask.h"

namespace kuzu {
namespace function {

// TODO(Semih): Remove
//class DenseFrontier {
//public:
//    DenseFrontier(std::vector<std::tuple<common::table_id_t, uint64_t>> nodeTableIDAndSizes) {
//        for (const auto& [tableID, size] : nodeTableIDAndSizes) {
//            masks.insert({tableID, std::make_unique<processor::AutoIncrementedMaskData>(size)});
//        }
//    }
//
//    void setNode(common::offset_t nodeOffset) {
//        KU_ASSERT(activeMask != nullptr);
//        activeMask->setMask(nodeOffset);
//    }
//
//    void setActiveMask(common::table_id_t tableID) {
//        KU_ASSERT(masks.contains(tableID));
//        activeMask = masks.at(tableID).get();
//    }
//
//    void finalizeFrontierLevel() {
//        activeMask->incrementMaskValue();
//    }
//
//private:
//    common::table_id_map_t<std::unique_ptr<processor::AutoIncrementedMaskData>> masks;
//    processor::AutoIncrementedMaskData* activeMask;
//};

} // namespace function
} // namespace kuzu
