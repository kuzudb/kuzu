#pragma once

#include "processor/operator/mask.h"
#include "processor/operator/recursive_extend/bfs_state.h"
#include "processor/operator/result_collector.h"

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
 *    This policy aims to support at any given point at most 'k' total active SSSPMorsels. Any
 *    thread can be assigned to work any of the active SSPMorsels. A different SSSPMorsel gets
 *    assigned only if the already assigned SSSPMorsel does not have work or is completed. The work
 *    can be either scan extend or writing path length to vector.
 */
enum SchedulerType { OneThreadOneMorsel, nThreadkMorsel };

struct MorselDispatcher {
public:
    MorselDispatcher(SchedulerType schedulerType, uint64_t lowerBound, uint64_t upperBound,
        uint64_t maxOffset, uint64_t numThreadsForExecution)
        : schedulerType{schedulerType}, threadIdxCounter{0u}, numActiveSSSP{0u},
          activeSSSPMorsel{std::vector<std::shared_ptr<SSSPMorsel>>(numThreadsForExecution)},
          globalState{IN_PROGRESS}, maxOffset{maxOffset}, lowerBound{lowerBound}, upperBound{
                                                                                      upperBound} {}

    uint32_t getThreadIdx();

    // Not thread safe, called only for initialization of BFSMorsel. ThreadIdx position is fixed.
    SSSPMorsel* getSSSPMorsel(uint32_t threadIdx);

    std::pair<GlobalSSSPState, SSSPLocalState> getBFSMorsel(
        const std::shared_ptr<FTableSharedState>& inputFTableSharedState,
        const std::vector<common::ValueVector*> vectorsToScan,
        const std::vector<ft_col_idx_t> colIndicesToScan, common::ValueVector* srcNodeIDVector,
        std::unique_ptr<BaseBFSMorsel>& bfsMorsel, uint32_t threadIdx);

    int64_t getNextAvailableSSSPWork(uint32_t threadIdx);

    std::pair<GlobalSSSPState, SSSPLocalState> findAvailableSSSPMorsel(
        std::unique_ptr<BaseBFSMorsel>& bfsMorsel, SSSPLocalState& ssspLocalState,
        uint32_t threadIdx);

    int64_t writeDstNodeIDAndDistance(
        const std::shared_ptr<FTableSharedState>& inputFTableSharedState,
        std::vector<common::ValueVector*> vectorsToScan, std::vector<ft_col_idx_t> colIndicesToScan,
        common::ValueVector* dstNodeIDVector, common::ValueVector* distanceVector,
        common::table_id_t tableID, uint32_t threadIdx);

    inline SchedulerType getSchedulerType() { return schedulerType; }

private:
    SchedulerType schedulerType;
    common::offset_t maxOffset;
    uint64_t upperBound;
    uint64_t lowerBound;
    uint32_t threadIdxCounter;
    std::vector<std::shared_ptr<SSSPMorsel>> activeSSSPMorsel;
    uint32_t numActiveSSSP;
    GlobalSSSPState globalState;
    std::mutex mutex;
};

} // namespace processor
} // namespace kuzu
