#pragma once

#include "bfs_state.h"
#include "processor/operator/physical_operator.h"
#include "storage/store/node_table.h"

namespace kuzu {
namespace processor {

struct PayloadInfo {
    DataPos dataPos;
    common::DataType dataType;
    bool isFlat;

    PayloadInfo(const DataPos& dataPos, const common::DataType& dataType, bool isFlat)
        : dataPos{dataPos}, dataType{dataType}, isFlat{isFlat} {}
};

class ScanBFSLevel : public PhysicalOperator {
public:
    ScanBFSLevel(uint8_t upperBound, storage::NodeTable* nodeTable,
        const DataPos& srcNodeIDVectorPos, const DataPos& dstNodeIDVectorPos,
        const DataPos& distanceVectorPos, std::vector<PayloadInfo> payloadInfos,
        std::shared_ptr<FTableSharedState> globalSharedState,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::SCAN_BFS_LEVEL, std::move(child), id,
              paramsString},
          upperBound{upperBound}, nodeTable{nodeTable}, srcNodeIDVectorPos{srcNodeIDVectorPos},
          dstNodeIDVectorPos{dstNodeIDVectorPos}, distanceVectorPos{distanceVectorPos},
          payloadInfos{std::move(payloadInfos)}, globalSharedState{std::move(globalSharedState)} {}

    inline void setThreadLocalSharedState(std::shared_ptr<SSPThreadLocalSharedState> state) {
        threadLocalSharedState = std::move(state);
    }

    void initGlobalStateInternal(ExecutionContext* context) override;

    void initLocalStateInternal(ResultSet* resultSet_, ExecutionContext* context) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<ScanBFSLevel>(upperBound, nodeTable, srcNodeIDVectorPos,
            dstNodeIDVectorPos, distanceVectorPos, payloadInfos, globalSharedState,
            children[0]->clone(), id, paramsString);
    }

private:
    void writeDstNodeIDsAndDstToFTable();
    void writeDstNodeIDsAndDstToVector(common::offset_t& currentOffset, size_t size);

    std::unique_ptr<FactorizedTableSchema> populateTableSchema();

private:
    uint8_t upperBound;
    storage::NodeTable* nodeTable;
    DataPos srcNodeIDVectorPos;
    DataPos dstNodeIDVectorPos;
    DataPos distanceVectorPos;
    std::vector<PayloadInfo> payloadInfos;
    std::shared_ptr<FTableSharedState> globalSharedState;
    std::shared_ptr<SSPThreadLocalSharedState> threadLocalSharedState;

    common::offset_t maxNodeOffset;
    std::shared_ptr<common::ValueVector> srcNodeIDVector;
    std::shared_ptr<common::ValueVector> dstNodeIDVector;
    std::shared_ptr<common::ValueVector> distanceVector;
    std::vector<common::ValueVector*> vectorsToCollect;
};

} // namespace processor
} // namespace kuzu