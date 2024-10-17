#pragma once

#include "binder/ddl/bound_create_sequence_info.h"
#include "planner/operator/ddl/logical_ddl.h"

namespace kuzu {
namespace planner {

struct LogicalCreateSequencePrintInfo final : OPPrintInfo {
    std::string sequenceName;

    explicit LogicalCreateSequencePrintInfo(std::string sequenceName)
        : sequenceName(std::move(sequenceName)) {}

    std::string toString() const override { return "Sequence: " + sequenceName; };

    std::unique_ptr<OPPrintInfo> copy() const override {
        return std::unique_ptr<LogicalCreateSequencePrintInfo>(
            new LogicalCreateSequencePrintInfo(*this));
    }

private:
    LogicalCreateSequencePrintInfo(const LogicalCreateSequencePrintInfo& other)
        : OPPrintInfo(other), sequenceName(other.sequenceName) {}
};

class LogicalCreateSequence : public LogicalDDL {
public:
    LogicalCreateSequence(std::string sequenceName, binder::BoundCreateSequenceInfo info,
        std::shared_ptr<binder::Expression> outputExpression)
        : LogicalDDL{LogicalOperatorType::CREATE_SEQUENCE, std::move(sequenceName),
              std::move(outputExpression)},
          info{std::move(info)} {}

    binder::BoundCreateSequenceInfo getInfo() const { return info.copy(); }

    std::unique_ptr<OPPrintInfo> getPrintInfo() const override {
        return std::make_unique<LogicalCreateSequencePrintInfo>(tableName);
    }

    inline std::unique_ptr<LogicalOperator> copy() final {
        return std::make_unique<LogicalCreateSequence>(tableName, info.copy(), outputExpression);
    }

private:
    binder::BoundCreateSequenceInfo info;
};

} // namespace planner
} // namespace kuzu
