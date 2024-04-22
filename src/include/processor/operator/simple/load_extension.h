#pragma once

#include "processor/operator/simple/simple.h"

namespace kuzu {
namespace processor {

class LoadExtension final : public Simple {
public:
    LoadExtension(std::string path, const DataPos& outputPos, uint32_t id,
        const std::string& paramsString)
        : Simple{PhysicalOperatorType::LOAD_EXTENSION, outputPos, id, paramsString},
          path{std::move(path)} {}

    void executeInternal(ExecutionContext* context) override;
    std::string getOutputMsg() override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<LoadExtension>(path, outputPos, id, paramsString);
    }

private:
    std::string path;
};

} // namespace processor
} // namespace kuzu
