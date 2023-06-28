#pragma once

#include "main/db_config.h"
#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

struct CallConfigInfo {
    main::ConfigurationOption option;
    common::Value optionValue;
    bool hasExecuted = false;

    CallConfigInfo(main::ConfigurationOption option, const common::Value& optionValue)
        : option{std::move(option)}, optionValue{optionValue} {}

    std::unique_ptr<CallConfigInfo> copy() {
        return std::make_unique<CallConfigInfo>(option, optionValue);
    }
};

class CallConfig : public PhysicalOperator {
public:
    CallConfig(std::unique_ptr<CallConfigInfo> localState, PhysicalOperatorType operatorType,
        uint32_t id, const std::string& paramsString)
        : callConfigInfo{std::move(localState)}, PhysicalOperator{operatorType, id, paramsString} {}

    inline bool isSource() const override { return true; }

    bool getNextTuplesInternal(ExecutionContext* context) override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<CallConfig>(callConfigInfo->copy(), operatorType, id, paramsString);
    }

protected:
    std::unique_ptr<CallConfigInfo> callConfigInfo;
};

} // namespace processor
} // namespace kuzu
