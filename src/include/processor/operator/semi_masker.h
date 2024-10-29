#pragma once

#include "common/enums/extend_direction.h"
#include "common/mask.h"
#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

class BaseSemiMasker;

using mask_vector = std::vector<common::RoaringBitmapSemiMask*>;

struct SemiMaskerLocalInfo {
    std::unordered_map<common::table_id_t, std::unique_ptr<common::RoaringBitmapSemiMask>>
        localMasksPerTable;
    common::RoaringBitmapSemiMask* singleTableRef = nullptr;
};

struct SemiMaskerInnerInfo {
    std::unordered_map<common::table_id_t, mask_vector> masksPerTable;
    std::vector<std::shared_ptr<SemiMaskerLocalInfo>> localInfos;
    std::mutex mtx;
};

class SemiMaskerInfo {
    friend class BaseSemiMasker;

public:
    SemiMaskerInfo(const DataPos& keyPos,
        std::unordered_map<common::table_id_t, mask_vector> masksPerTable)
        : keyPos{keyPos} {
        innerInfo = std::make_shared<SemiMaskerInnerInfo>();
        innerInfo->masksPerTable = std::move(masksPerTable);
    }

    SemiMaskerInfo(const SemiMaskerInfo& other)
        : keyPos{other.keyPos}, innerInfo{other.innerInfo} {}

    std::shared_ptr<SemiMaskerLocalInfo> appendLocalInfo() {
        auto localInfo = std::make_shared<SemiMaskerLocalInfo>();
        bool isSingle = innerInfo->masksPerTable.size() == 1;
        for (const auto& [tableID, vector] : innerInfo->masksPerTable) {
            auto& mask = vector.front();
            auto newOne = common::RoaringBitmapSemiMaskUtil::createRoaringBitmapSemiMask(
                mask->getTableID(), mask->getMaxOffset());
            if (isSingle) {
                localInfo->singleTableRef = newOne.get();
            }
            localInfo->localMasksPerTable.insert({tableID, std::move(newOne)});
        }

        std::unique_lock lock{innerInfo->mtx};
        innerInfo->localInfos.push_back(localInfo);
        return localInfo;
    }

    void mergeToGlobalInfo() {
        for (const auto& [tableID, globalVector] : innerInfo->masksPerTable) {
            if (globalVector.front()->getMaxOffset() > std::numeric_limits<uint32_t>::max()) {
                std::vector<roaring::Roaring64Map*> masks;
                for (const auto& localInfo : innerInfo->localInfos) {
                    const auto& mask = localInfo->localMasksPerTable.at(tableID);
                    auto mask64 = static_cast<common::Roaring64BitmapSemiMask*>(mask.get());
                    if (!mask64->roaring->isEmpty()) {
                        masks.push_back(mask64->roaring.get());
                    }
                }
                auto mergedMask = std::make_shared<roaring::Roaring64Map>(
                    roaring::Roaring64Map::fastunion(masks.size(),
                        const_cast<const roaring::Roaring64Map**>(masks.data())));
                for (const auto& item : globalVector) {
                    auto mask64 = static_cast<common::Roaring64BitmapSemiMask*>(item);
                    mask64->roaring = mergedMask;
                }
            } else {
                std::vector<roaring::Roaring*> masks;
                for (const auto& localInfo : innerInfo->localInfos) {
                    const auto& mask = localInfo->localMasksPerTable.at(tableID);
                    auto mask32 = static_cast<common::Roaring32BitmapSemiMask*>(mask.get());
                    if (!mask32->roaring->isEmpty()) {
                        masks.push_back(mask32->roaring.get());
                    }
                }
                auto mergedMask = std::make_shared<roaring::Roaring>(roaring::Roaring::fastunion(
                    masks.size(), const_cast<const roaring::Roaring**>(masks.data())));
                for (const auto& item : globalVector) {
                    auto mask32 = static_cast<common::Roaring32BitmapSemiMask*>(item);
                    mask32->roaring = mergedMask;
                }
            }
        }
    }

    std::unique_ptr<SemiMaskerInfo> copy() const { return std::make_unique<SemiMaskerInfo>(*this); }

private:
    DataPos keyPos;
    std::shared_ptr<SemiMaskerInnerInfo> innerInfo;
};

struct SemiMaskerPrintInfo final : OPPrintInfo {
    std::vector<std::string> operatorNames;

    explicit SemiMaskerPrintInfo(std::vector<std::string> operatorNames)
        : operatorNames{std::move(operatorNames)} {}

    std::string toString() const override;

    std::unique_ptr<OPPrintInfo> copy() const override {
        return std::unique_ptr<SemiMaskerPrintInfo>(new SemiMaskerPrintInfo(*this));
    }

private:
    SemiMaskerPrintInfo(const SemiMaskerPrintInfo& other)
        : OPPrintInfo{other}, operatorNames{other.operatorNames} {}
};

class BaseSemiMasker : public PhysicalOperator {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::SEMI_MASKER;

protected:
    BaseSemiMasker(std::unique_ptr<SemiMaskerInfo> info, std::unique_ptr<PhysicalOperator> child,
        uint32_t id, std::unique_ptr<OPPrintInfo> printInfo)
        : PhysicalOperator{type_, std::move(child), id, std::move(printInfo)},
          info{std::move(info)}, keyVector{nullptr} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    void finalizeInternal(ExecutionContext* context) final;

protected:
    std::unique_ptr<SemiMaskerInfo> info;
    common::ValueVector* keyVector;
    std::shared_ptr<SemiMaskerLocalInfo> localInfo;
};

class SingleTableSemiMasker : public BaseSemiMasker {
public:
    SingleTableSemiMasker(std::unique_ptr<SemiMaskerInfo> info,
        std::unique_ptr<PhysicalOperator> child, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : BaseSemiMasker{std::move(info), std::move(child), id, std::move(printInfo)} {}

    bool getNextTuplesInternal(ExecutionContext* context) final;

    std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<SingleTableSemiMasker>(info->copy(), children[0]->clone(), id,
            printInfo->copy());
    }
};

class MultiTableSemiMasker : public BaseSemiMasker {
public:
    MultiTableSemiMasker(std::unique_ptr<SemiMaskerInfo> info,
        std::unique_ptr<PhysicalOperator> child, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : BaseSemiMasker{std::move(info), std::move(child), id, std::move(printInfo)} {}

    bool getNextTuplesInternal(ExecutionContext* context) final;

    std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<MultiTableSemiMasker>(info->copy(), children[0]->clone(), id,
            printInfo->copy());
    }
};

class PathSemiMasker : public BaseSemiMasker {
protected:
    PathSemiMasker(std::unique_ptr<SemiMaskerInfo> info, std::unique_ptr<PhysicalOperator> child,
        uint32_t id, std::unique_ptr<OPPrintInfo> printInfo, common::ExtendDirection direction)
        : BaseSemiMasker{std::move(info), std::move(child), id, std::move(printInfo)},
          pathRelsVector{nullptr}, pathRelsSrcIDDataVector{nullptr},
          pathRelsDstIDDataVector{nullptr}, direction{std::move(direction)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) final;

protected:
    common::ValueVector* pathRelsVector;
    common::ValueVector* pathRelsSrcIDDataVector;
    common::ValueVector* pathRelsDstIDDataVector;
    common::ExtendDirection direction;
};

class PathSingleTableSemiMasker : public PathSemiMasker {
public:
    PathSingleTableSemiMasker(std::unique_ptr<SemiMaskerInfo> info,
        std::unique_ptr<PhysicalOperator> child, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo, common::ExtendDirection direction)
        : PathSemiMasker{std::move(info), std::move(child), id, std::move(printInfo),
              std::move(direction)} {}

    bool getNextTuplesInternal(ExecutionContext* context) final;

    std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<PathSingleTableSemiMasker>(info->copy(), children[0]->clone(), id,
            printInfo->copy(), direction);
    }
};

class PathMultipleTableSemiMasker : public PathSemiMasker {
public:
    PathMultipleTableSemiMasker(std::unique_ptr<SemiMaskerInfo> info,
        std::unique_ptr<PhysicalOperator> child, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo, common::ExtendDirection direction)
        : PathSemiMasker{std::move(info), std::move(child), id, std::move(printInfo),
              std::move(direction)} {}

    bool getNextTuplesInternal(ExecutionContext* context) final;

    std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<PathMultipleTableSemiMasker>(info->copy(), children[0]->clone(), id,
            printInfo->copy(), direction);
    }
};

} // namespace processor
} // namespace kuzu
