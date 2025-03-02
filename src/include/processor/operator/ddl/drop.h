#pragma once

#include "parser/ddl/drop_info.h"
#include "processor/operator/simple/simple.h"

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

class Drop final : public Simple {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::DROP;

public:
    Drop(parser::DropInfo dropInfo, const DataPos& outputPos, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : Simple{type_, outputPos, id, std::move(printInfo)}, dropInfo{std::move(dropInfo)},
          entryDropped{false} {}

    void executeInternal(ExecutionContext* context) override;

    std::string getOutputMsg() override;

    std::unique_ptr<PhysicalOperator> copy() override {
        return make_unique<Drop>(dropInfo, outputPos, id, printInfo->copy());
    }

private:
    void dropSequence(const main::ClientContext* context);
    void dropTable(const main::ClientContext* context);
    void dropRelGroup(const main::ClientContext* context);

private:
    parser::DropInfo dropInfo;
    bool entryDropped;
};

} // namespace processor
} // namespace kuzu
