#pragma once

#include "processor/operator/simple/simple.h"

namespace kuzu {
namespace processor {

struct CreateTypePrintInfo final : OPPrintInfo {
    std::string typeName;
    std::string type;

    CreateTypePrintInfo(std::string typeName, std::string type)
        : typeName{std::move(typeName)}, type{std::move(type)} {}

    std::string toString() const override;

    std::unique_ptr<OPPrintInfo> copy() const override {
        return std::unique_ptr<CreateTypePrintInfo>(new CreateTypePrintInfo(*this));
    }

private:
    CreateTypePrintInfo(const CreateTypePrintInfo& other)
        : OPPrintInfo{other}, typeName{other.typeName}, type{other.type} {}
};

class CreateType final : public Simple {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::CREATE_TYPE;

public:
    CreateType(std::string name, common::LogicalType type, DataPos outputPos, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : Simple{type_, outputPos, id, std::move(printInfo)}, name{std::move(name)},
          type{std::move(type)} {}

    void executeInternal(ExecutionContext* context) override;

    std::string getOutputMsg() override;

    std::unique_ptr<PhysicalOperator> copy() override {
        return std::make_unique<CreateType>(name, type.copy(), outputPos, id, printInfo->copy());
    }

private:
    std::string name;
    common::LogicalType type;
};

} // namespace processor
} // namespace kuzu
