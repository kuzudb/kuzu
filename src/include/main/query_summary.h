#pragma once

#include "flat_tuple.h"
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
    inline double getCompilingTime() const { return preparedSummary.compilingTime; }

    inline double getExecutionTime() const { return executionTime; }

    inline bool getIsExplain() const { return preparedSummary.isExplain; }

    inline bool getIsProfile() const { return preparedSummary.isProfile; }

    inline std::ostringstream& getPlanAsOstream() { return planInOstream; }

    inline void setPreparedSummary(PreparedSummary preparedSummary_) {
        preparedSummary = preparedSummary_;
    }

    nlohmann::json& printPlanToJson();

private:
    double executionTime = 0;
    PreparedSummary preparedSummary;
    std::unique_ptr<nlohmann::json> planInJson;
    std::ostringstream planInOstream;
};

} // namespace main
} // namespace kuzu
