#pragma once

#include "processor/operator/physical_operator.h"
#include "processor/operator/result_collector.h"
#include "processor/result/factorized_table.h"

using namespace kuzu::processor;

namespace kuzu {
namespace processor {

enum VisitedState { NOT_VISITED, NOT_VISITED_DST, VISITED, VISITED_DST };

/*
 * Current Implementation:
 * Only 1 thread is assigned to each SSSP computation. This ensures that at any point in time 1
 * thread works on 1 BFSLevel.
 */
struct BFSLevel {
public:
    BFSLevel() : bfsLevelNodes{std::vector<common::nodeID_t>()}, levelNumber{0u} {}

    inline bool isEmpty() const { return bfsLevelNodes.empty(); }

public:
    std::vector<common::nodeID_t> bfsLevelNodes;
    uint32_t levelNumber;
};

/*
 * Operator function to sort bfsLevelNodes nodeID's in ascending order.
 */
struct NodeIDComparatorFunction {
    inline bool operator()(const common::nodeID_t& nodeID1, const common::nodeID_t& nodeID2) {
        return nodeID1.offset < nodeID2.offset;
    }
};

struct BFSLevelMorsel {
public:
    BFSLevelMorsel(uint32_t startIdx, uint32_t size) : startIdx{startIdx}, size{size} {}

    inline bool isEmpty() const { return size == 0u; }

public:
    uint32_t startIdx;
    uint32_t size;
};

struct SSSPMorsel {
public:
    explicit SSSPMorsel(common::offset_t maxNodeOffset)
        : isWrittenToOutFTable{false}, numDstNodesNotReached{0u}, bfsMorselNextStartIdx{0u},
          dstTableID{0u}, dstDistances{std::unordered_map<common::offset_t, uint32_t>()},
          curBFSLevel{std::make_unique<BFSLevel>()}, nextBFSLevel{std::make_unique<BFSLevel>()},
          bfsVisitedNodes{std::vector<uint8_t>(maxNodeOffset + 1, NOT_VISITED)} {}

    BFSLevelMorsel getBFSLevelMorsel();

    void markDstNodeOffsets(
        common::offset_t srcNodeOffset, common::ValueVector* dstNodeIDValueVector);

    inline bool isComplete(uint8_t upperBound) const {
        return (numDstNodesNotReached == 0 || upperBound == curBFSLevel->levelNumber ||
                curBFSLevel->bfsLevelNodes.empty());
    }

public:
    std::shared_mutex mutex;
    bool isWrittenToOutFTable;
    uint32_t numDstNodesNotReached;
    uint32_t bfsMorselNextStartIdx;
    common::table_id_t dstTableID;
    std::unordered_map<common::offset_t, uint32_t> dstDistances;
    std::unique_ptr<BFSLevel> curBFSLevel;
    std::unique_ptr<BFSLevel> nextBFSLevel;
    // Each element is of size 1 byte (unsigned char) and enum VisitedState states are stored.
    // We have 4 states currently, and it gets cast to uint8_t in the vector.
    std::vector<uint8_t> bfsVisitedNodes;
};

struct SSSPMorselTracker {
public:
    explicit SSSPMorselTracker(std::shared_ptr<FTableSharedState> inputFTable)
        : ssspMorselPerThread{std::unordered_map<std::thread::id, std::unique_ptr<SSSPMorsel>>()},
          inputFTable{std::move(inputFTable)} {};

    SSSPMorsel* getAssignedSSSPMorsel(std::thread::id threadID);

    void removePrevAssignedSSSPMorsel(std::thread::id threadID);

    SSSPMorsel* getSSSPMorsel(std::thread::id threadID, common::offset_t maxNodeOffset,
        std::vector<common::ValueVector*> srcDstValueVectors,
        std::vector<uint32_t>& ftColIndicesToScan);

private:
    std::shared_mutex mutex;
    std::unordered_map<std::thread::id, std::unique_ptr<SSSPMorsel>> ssspMorselPerThread;
    std::shared_ptr<FTableSharedState> inputFTable;
};

class ScanBFSLevel : public PhysicalOperator {

public:
    ScanBFSLevel(common::offset_t maxNodeOffset, const DataPos& nodesToExtendDataPos,
        std::vector<DataPos> srcDstVectorsDataPos, std::vector<uint32_t> ftColIndicesToScan,
        uint8_t upperBound, std::shared_ptr<SSSPMorselTracker> ssspMorselTracker,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : PhysicalOperator(
              PhysicalOperatorType::SCAN_BFS_LEVEL, std::move(child), id, paramsString),
          maxNodeOffset{maxNodeOffset}, nodesToExtendDataPos{nodesToExtendDataPos},
          srcDstVectorsDataPos{std::move(srcDstVectorsDataPos)}, ftColIndicesToScan{std::move(
                                                                     ftColIndicesToScan)},
          upperBound{upperBound}, ssspMorsel{nullptr}, ssspMorselTracker{
                                                           std::move(ssspMorselTracker)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool isSource() const override { return true; }

    bool getNextTuplesInternal(ExecutionContext* context) override;

    void copyCurBFSLevelNodesToVector(BFSLevel& curBFSLevel, BFSLevelMorsel& bfsLevelMorsel);

    std::shared_ptr<SSSPMorselTracker>& getSSSPMorselTracker() { return ssspMorselTracker; }

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<ScanBFSLevel>(maxNodeOffset, nodesToExtendDataPos,
            srcDstVectorsDataPos, ftColIndicesToScan, upperBound, ssspMorselTracker,
            children[0]->clone(), id, paramsString);
    }

private:
    std::thread::id threadID;
    common::offset_t maxNodeOffset;
    // The ValueVector into which ScanBFSLevel will write the nodes to be extended.
    DataPos nodesToExtendDataPos;
    std::shared_ptr<common::ValueVector> nodesToExtend;
    // The ValueVectors into which the src, dst ID & properties will be written.
    // TODO: This can be optimized by directly copying from the input FTable to the output FTable,
    // instead of copying into these value vectors and then copying them to the output FTable.
    std::vector<DataPos> srcDstVectorsDataPos;
    std::vector<common::ValueVector*> srcDstValueVectors;
    // The FTable column indices for the src, dest nodeIDs and node properties to scan.
    std::vector<uint32_t> ftColIndicesToScan;
    uint8_t upperBound;
    SSSPMorsel* ssspMorsel;
    std::shared_ptr<SSSPMorselTracker> ssspMorselTracker;
};
} // namespace processor
} // namespace kuzu
