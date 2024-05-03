#pragma once

#include "common/types/value/value.h"
#include "main/db_config.h"
#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

struct StandaloneCallInfo {
    main::Option* option;
    common::Value optionValue;
    bool hasExecuted = false;

    StandaloneCallInfo(main::Option* option, const common::Value& optionValue)
        : option{option}, optionValue{optionValue} {}

    std::unique_ptr<StandaloneCallInfo> copy() {
        return std::make_unique<StandaloneCallInfo>(option, optionValue);
    }
};

class StandaloneCall : public PhysicalOperator {
public:
    StandaloneCall(std::unique_ptr<StandaloneCallInfo> localState,
        PhysicalOperatorType operatorType, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{operatorType, id, paramsString},
          standaloneCallInfo{std::move(localState)} {}

    bool isSource() const override { return true; }
    bool canParallel() const final { return false; }

    bool getNextTuplesInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<StandaloneCall>(standaloneCallInfo->copy(), operatorType, id,
            paramsString);
    }

private:
    std::unique_ptr<StandaloneCallInfo> standaloneCallInfo;
};

} // namespace processor
} // namespace kuzu
