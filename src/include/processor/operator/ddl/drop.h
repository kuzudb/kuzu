#pragma once

#include "ddl.h"
#include "parser/ddl/drop_info.h"

namespace kuzu {
namespace processor {

struct DropPrintInfo final : OPPrintInfo {
    std::string name;

    explicit DropPrintInfo(std::string name) : name{std::move(name)} {}

    std::string toString() const override { return name; }

    std::unique_ptr<OPPrintInfo> copy() const override {
        return std::unique_ptr<DropPrintInfo>(new DropPrintInfo(*this));
    }

private:
    DropPrintInfo(const DropPrintInfo& other) : OPPrintInfo{other}, name{other.name} {}
};

class Drop : public DDL {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::DROP;

public:
    Drop(parser::DropInfo dropInfo, const DataPos& outputPos, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : DDL{type_, outputPos, id, std::move(printInfo)}, dropInfo{std::move(dropInfo)},
          validEntry{false} {}

    void executeDDLInternal(ExecutionContext* context) override;

    std::string getOutputMsg() override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<Drop>(dropInfo, outputPos, id, printInfo->copy());
    }

private:
    parser::DropInfo dropInfo;
    bool validEntry;
};

} // namespace processor
} // namespace kuzu
