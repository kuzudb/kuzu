#include "main/query_summary.h"

#include <json.hpp>

namespace kuzu {
namespace main {

nlohmann::json& QuerySummary::printPlanToJson() {
    return *planInJson;
}

} // namespace main
} // namespace kuzu
