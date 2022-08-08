#pragma once

#include "base_logical_operator.h"

#include "src/common/include/csv_reader/csv_reader.h"

namespace graphflow {
namespace planner {

class LogicalCopyCSV : public LogicalOperator {

public:
    LogicalCopyCSV(const CSVDescription csvDescription)
        : LogicalOperator{}, csvDescription{move(csvDescription)} {}

    inline LogicalOperatorType getLogicalOperatorType() const override { return LOGICAL_COPY_CSV; }

    inline string getExpressionsForPrinting() const override {
        return to_string(csvDescription.labelID);
    }

    inline CSVDescription getCSVDescription() const { return csvDescription; }

    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalCopyCSV>(csvDescription);
    }

private:
    const CSVDescription csvDescription;
};

} // namespace planner
} // namespace graphflow
