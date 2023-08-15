#pragma once

#include <queue>

#include "processor/operator/mask.h"
#include "processor/operator/recursive_extend/bfs_state.h"
#include "processor/operator/result_collector.h"
#include "processor/operator/table_scan/factorized_table_scan.h"

namespace kuzu {
namespace processor {

struct MorselDispatcher {
public:
    MorselDispatcher(common::SchedulerType schedulerType, uint64_t lowerBound, uint64_t upperBound,
        uint64_t maxOffset)
        : schedulerType{schedulerType}, numActiveBFSSharedState{0u}, globalState{IN_PROGRESS},
          maxOffset{maxOffset}, lowerBound{lowerBound}, upperBound{upperBound} {}

    inline void initActiveBFSSharedState(uint32_t numThreads) {
        activeBFSSharedState = std::vector<std::shared_ptr<BFSSharedState>>(numThreads, nullptr);
    }

    std::pair<GlobalSSSPState, SSSPLocalState> getBFSMorsel(
        const std::shared_ptr<FactorizedTableScanSharedState>& inputFTableSharedState,
        const std::vector<common::ValueVector*> vectorsToScan,
        const std::vector<ft_col_idx_t> colIndicesToScan, common::ValueVector* srcNodeIDVector,
        BaseBFSMorsel* bfsMorsel, common::QueryRelType queryRelType);

    static void setUpNewBFSSharedState(std::shared_ptr<BFSSharedState>& newBFSSharedState,
        BaseBFSMorsel* bfsMorsel, FactorizedTableScanMorsel* inputFTableMorsel,
        common::nodeID_t nodeID, common::QueryRelType queryRelType);

    uint32_t getNextAvailableSSSPWork();

    std::pair<GlobalSSSPState, SSSPLocalState> findAvailableSSSP(BaseBFSMorsel* bfsMorsel);

    int64_t writeDstNodeIDAndPathLength(
        const std::shared_ptr<FactorizedTableScanSharedState>& inputFTableSharedState,
        std::vector<common::ValueVector*> vectorsToScan, std::vector<ft_col_idx_t> colIndicesToScan,
        common::ValueVector* dstNodeIDVector, common::ValueVector* pathLengthVector,
        common::table_id_t tableID, std::unique_ptr<BaseBFSMorsel>& baseBfsMorsel);

    inline common::SchedulerType getSchedulerType() { return schedulerType; }

    inline void setSchedulerType(common::SchedulerType schedulerType_) {
        schedulerType = schedulerType_;
    }

private:
    common::SchedulerType schedulerType;
    common::offset_t maxOffset;
    uint64_t upperBound;
    uint64_t lowerBound;
    std::vector<std::shared_ptr<BFSSharedState>> activeBFSSharedState;
    uint32_t numActiveBFSSharedState;
    GlobalSSSPState globalState;
    std::mutex mutex;
};

} // namespace processor
} // namespace kuzu
