#pragma once

#include "processor/operator/scan/scan_rel_table.h"

namespace kuzu {
namespace processor {

struct DirectionInfo {
    bool extendFromSource;
    DataPos directionPos;

    DirectionInfo() : extendFromSource{false}, directionPos{DataPos::getInvalidPos()} {}
    EXPLICIT_COPY_DEFAULT_MOVE(DirectionInfo);

    bool needFlip(common::RelDataDirection relDataDirection) const;

private:
    DirectionInfo(const DirectionInfo& other)
        : extendFromSource{other.extendFromSource}, directionPos{other.directionPos} {}
};

class RelTableCollectionScanner {
    friend class ScanMultiRelTable;

public:
    explicit RelTableCollectionScanner(std::vector<ScanRelTableInfo> relInfos)
        : relInfos{std::move(relInfos)} {
        initActivationRelInfos();
    }
    EXPLICIT_COPY_DEFAULT_MOVE(RelTableCollectionScanner);

    bool empty() const { return relInfos.empty(); }

    void resetState() {
        currentTableIdx = 0;
        nextTableIdx = 0;
    }

    bool scan(transaction::Transaction* transaction);

    void setActivationRelInfo(const std::vector<size_t>& activationRelInfoIndex) {
        activationRelInfos.clear();
        for (const auto& index : activationRelInfoIndex) {
            activationRelInfos.push_back(&relInfos.at(index));
        }
        if (!directionValues.empty()) {
            activationDirectionValues.clear();
            for (const auto& index : activationRelInfoIndex) {
                activationDirectionValues.push_back(directionValues.at(index));
            }
        }
    }
    const std::vector<ScanRelTableInfo>& getRelInfos() const { return relInfos; }

    void addDirectionValue(bool value) {
        directionValues.push_back(value);
        activationDirectionValues.push_back(value);
    }

private:
    RelTableCollectionScanner(const RelTableCollectionScanner& other)
        : relInfos{copyVector(other.relInfos)} {
        initActivationRelInfos();
    }

    void initActivationRelInfos() {
        for (auto& item : relInfos) {
            activationRelInfos.push_back(&item);
        }
    }

private:
    std::vector<ScanRelTableInfo> relInfos;
    std::vector<ScanRelTableInfo*> activationRelInfos;
    std::vector<bool> directionValues;
    std::vector<bool> activationDirectionValues;
    common::ValueVector* directionVector = nullptr;
    common::idx_t currentTableIdx = common::INVALID_IDX;
    uint32_t nextTableIdx = 0;
};

class ScanMultiRelTable final : public ScanTable {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::SCAN_REL_TABLE;

public:
    ScanMultiRelTable(ScanTableInfo info, DirectionInfo directionInfo,
        common::table_id_map_t<RelTableCollectionScanner> scanners,
        std::unique_ptr<PhysicalOperator> child, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : ScanTable{type_, std::move(info), std::move(child), id, std::move(printInfo)},
          directionInfo{std::move(directionInfo)}, boundNodeIDVector{nullptr}, outState{nullptr},
          scanners{std::move(scanners)}, currentScanner{nullptr} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override;

    void setRecursiveActivationRelInfo(
        common::table_id_map_t<std::vector<size_t>> activationRelInfo) {
        for (auto& [tableID, activation] : activationRelInfo) {
            scanners.at(tableID).setActivationRelInfo(activation);
        }
    }

    const common::table_id_map_t<RelTableCollectionScanner>& getScanners() const {
        return scanners;
    }

private:
    void resetState();
    void initCurrentScanner(const common::nodeID_t& nodeID);
    void initVectors(storage::TableScanState& state, const ResultSet& resultSet) const override;

private:
    DirectionInfo directionInfo;
    common::ValueVector* boundNodeIDVector;
    common::DataChunkState* outState;
    common::table_id_map_t<RelTableCollectionScanner> scanners;
    RelTableCollectionScanner* currentScanner;
};

} // namespace processor
} // namespace kuzu
