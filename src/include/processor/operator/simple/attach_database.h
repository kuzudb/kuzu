#pragma once

#include "parser/parsed_data/attach_info.h"
#include "processor/operator/simple/simple.h"

namespace kuzu {
namespace processor {

class AttachDatabase final : public Simple {
public:
    AttachDatabase(parser::AttachInfo attachInfo, const DataPos& outputPos, uint32_t id,
        const std::string& paramsString)
        : Simple{PhysicalOperatorType::ATTACH_DATABASE, outputPos, id, paramsString},
          attachInfo{std::move(attachInfo)} {}

    void executeInternal(ExecutionContext* context) override;
    std::string getOutputMsg() override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<AttachDatabase>(attachInfo, outputPos, id, paramsString);
    }

private:
    parser::AttachInfo attachInfo;
};

} // namespace processor
} // namespace kuzu
