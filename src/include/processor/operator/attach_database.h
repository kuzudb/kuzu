#pragma once

#include "parser/parsed_data/attach_info.h"
#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

class AttachDatabase final : public PhysicalOperator {
public:
    AttachDatabase(parser::AttachInfo attachInfo, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::ATTACH_DATABASE, id, paramsString},
          attachInfo{std::move(attachInfo)} {}

    bool isSource() const override { return true; }
    bool canParallel() const override { return false; }

    bool getNextTuplesInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<AttachDatabase>(attachInfo, id, paramsString);
    }

private:
    parser::AttachInfo attachInfo;
};

} // namespace processor
} // namespace kuzu
