#pragma once

#include "base_logical_operator.h"

#include "src/common/include/csv_reader/csv_reader.h"

namespace graphflow {
namespace planner {

class LogicalCopyCSV : public LogicalOperator {

public:
    LogicalCopyCSV(CSVDescription csvDescription, Label label)
        : LogicalOperator{}, csvDescription{move(csvDescription)}, label{move(label)} {}

    inline LogicalOperatorType getLogicalOperatorType() const override { return LOGICAL_COPY_CSV; }

    inline string getExpressionsForPrinting() const override { return label.labelName; }

    inline CSVDescription getCSVDescription() const { return csvDescription; }

    inline Label getLabel() const { return label; }

    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalCopyCSV>(csvDescription, label);
    }

private:
    CSVDescription csvDescription;
    Label label;
};

} // namespace planner
} // namespace graphflow
