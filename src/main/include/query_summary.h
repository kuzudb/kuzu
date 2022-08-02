#pragma once

#include "plan_printer.h"

namespace graphflow {
namespace main {

class QuerySummary {
    friend class Connection;
    friend class PreparedStatement;

public:
    double getCompilingTime() const { return compilingTime; }

    double getExecutionTime() const { return executionTime; }

    bool getIsExplain() const { return isExplain; }

    bool getIsProfile() const { return isProfile; }
    void enableProfile()  { isProfile = true; }

    void printPlanToStdOut() { planPrinter->printPlanToShell(); }
    nlohmann::json printPlanToJson() { return planPrinter->printPlanToJson(); }

private:
    double compilingTime = 0;
    double executionTime = 0;
    bool isExplain = false;
    bool isProfile = false;
    std::unique_ptr<PlanPrinter> planPrinter;
};

} // namespace main
} // namespace graphflow
