#include "main/query_summary.h"

#include "json.hpp"

namespace kuzu {
namespace main {

double QuerySummary::getCompilingTime() const {
    return preparedSummary.compilingTime;
}

double QuerySummary::getExecutionTime() const {
    return executionTime;
}

bool QuerySummary::getIsExplain() const {
    return preparedSummary.isExplain;
}

bool QuerySummary::getIsProfile() const {
    return preparedSummary.isProfile;
}

std::ostringstream& QuerySummary::getPlanAsOstream() {
    return planInOstream;
}

nlohmann::json& QuerySummary::printPlanToJson() {
    return *planInJson;
}

std::string QuerySummary::getPlan() {
    return planInOstream.str();
}

void QuerySummary::setPreparedSummary(PreparedSummary preparedSummary_) {
    preparedSummary = preparedSummary_;
}

} // namespace main
} // namespace kuzu
