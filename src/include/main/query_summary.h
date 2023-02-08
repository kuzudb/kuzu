#pragma once

#include "common/api.h"
#include "json_fwd.hpp"
#include "kuzu_fwd.h"
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
    friend class benchmark::Benchmark;

public:
    KUZU_API double getCompilingTime() const;
    KUZU_API double getExecutionTime() const;
    bool getIsExplain() const;
    bool getIsProfile() const;
    std::ostringstream& getPlanAsOstream();
    KUZU_API std::string getPlan();
    void setPreparedSummary(PreparedSummary preparedSummary_);

private:
    nlohmann::json& printPlanToJson();

private:
    double executionTime = 0;
    PreparedSummary preparedSummary;
    std::unique_ptr<nlohmann::json> planInJson;
    std::ostringstream planInOstream;
};

} // namespace main
} // namespace kuzu
