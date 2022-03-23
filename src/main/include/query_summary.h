#pragma once

#include "plan_printer.h"

namespace graphflow {
namespace main {

class QuerySummary {
    friend class Connection;

public:
    double getCompilingTime() const { return compilingTime; }

    double getExecutionTime() const { return executionTime; }

    bool getIsExplain() const { return isExplain; }

    bool getIsProfile() const { return isProfile; }

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
