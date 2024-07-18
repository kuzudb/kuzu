#pragma once

#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

struct LimitPrintInfo final : OPPrintInfo {
    uint64_t limitNum;

    explicit LimitPrintInfo(uint64_t limitNum) : limitNum{std::move(limitNum)} {}

    std::string toString() const override;

    std::unique_ptr<OPPrintInfo> copy() const override {
        return std::unique_ptr<LimitPrintInfo>(new LimitPrintInfo(*this));
    }

private:
    LimitPrintInfo(const LimitPrintInfo& other) : OPPrintInfo{other}, limitNum{other.limitNum} {}
};

class Limit : public PhysicalOperator {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::LIMIT;

public:
    Limit(uint64_t limitNumber, std::shared_ptr<std::atomic_uint64_t> counter,
        uint32_t dataChunkToSelectPos, std::unordered_set<uint32_t> dataChunksPosInScope,
        std::unique_ptr<PhysicalOperator> child, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : PhysicalOperator{type_, std::move(child), id, std::move(printInfo)},
          limitNumber{limitNumber}, counter{std::move(counter)},
          dataChunkToSelectPos{dataChunkToSelectPos},
          dataChunksPosInScope(std::move(dataChunksPosInScope)) {}

    bool getNextTuplesInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<Limit>(limitNumber, counter, dataChunkToSelectPos, dataChunksPosInScope,
            children[0]->clone(), id, printInfo->copy());
    }

private:
    uint64_t limitNumber;
    std::shared_ptr<std::atomic_uint64_t> counter;
    uint32_t dataChunkToSelectPos;
    std::unordered_set<uint32_t> dataChunksPosInScope;
};

} // namespace processor
} // namespace kuzu
