#pragma once

#include "plan_printer.h"

namespace kuzu {
namespace main {

struct PreparedSummary {
    double compilingTime = 0;
    bool isExplain = false;
    bool isProfile = false;
};

class QuerySummary {
    friend class Connection;
    friend class PreparedStatement;

public:
    double getCompilingTime() const { return preparedSummary.compilingTime; }

    double getExecutionTime() const { return executionTime; }

    bool getIsExplain() const { return preparedSummary.isExplain; }

    bool getIsProfile() const { return preparedSummary.isProfile; }

    std::ostringstream& getPlanAsOstream() { return planInOstream; }
    nlohmann::json& printPlanToJson() { return planInJson; }

    void setPreparedSummary(PreparedSummary preparedSummary) {
        this->preparedSummary = preparedSummary;
    }

private:
    double executionTime = 0;
    PreparedSummary preparedSummary;
    nlohmann::json planInJson;
    std::ostringstream planInOstream;
};

} // namespace main
} // namespace kuzu
