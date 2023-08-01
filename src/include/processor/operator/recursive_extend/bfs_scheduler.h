#pragma once

#include <queue>

#include "processor/operator/mask.h"
#include "processor/operator/recursive_extend/bfs_state.h"
#include "processor/operator/result_collector.h"
#include "processor/operator/table_scan/factorized_table_scan.h"

namespace kuzu {
namespace processor {

/**
 * Describing the scheduling policies currently being supported -
 *
 * 1) OneThreadOneMorsel (1T1S): This policy will be used for all multi label and path tracking
 *    queries. Each thread asking for work gets allotted its private morsel on which only that
 *    thread will work. Once that work is finished, we look for another morsel and if there are
 *    none the thread will exit.
 *
 * 2) nThreadkMorsel (nTkS): This policy will be used *ONLY* for single label - shortest path -
 *    no path tracking queries. Currently 'k' is the same as no. of threads active (or 'n').
 *    This policy aims to support at any given point at most 'k' total active BFSSharedStates. Any
 *    thread can be assigned to work any of the active SSPMorsels. A different BFSSharedState gets
 *    assigned only if the already assigned BFSSharedState does not have work or is completed. The
 * work can be either scan extend or writing path length to vector.
 */
enum SchedulerType { OneThreadOneMorsel, nThreadkMorsel };

struct MorselDispatcher {
public:
    MorselDispatcher(
        SchedulerType schedulerType, uint64_t lowerBound, uint64_t upperBound, uint64_t maxOffset)
        : schedulerType{schedulerType}, numActiveSSSP{0u}, globalState{IN_PROGRESS},
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

    inline SchedulerType getSchedulerType() { return schedulerType; }

private:
    SchedulerType schedulerType;
    common::offset_t maxOffset;
    uint64_t upperBound;
    uint64_t lowerBound;
    std::vector<std::shared_ptr<BFSSharedState>> activeBFSSharedState;
    uint32_t numActiveSSSP;
    GlobalSSSPState globalState;
    std::mutex mutex;
};

} // namespace processor
} // namespace kuzu
