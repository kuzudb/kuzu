#pragma once

#include "src/common/include/profiler.h"
#include "src/planner/logical_plan/include/logical_plan.h"
#include "src/processor/include/physical_plan/physical_plan.h"

using namespace graphflow::planner;
using namespace graphflow::processor;

namespace graphflow {
namespace main {

class OpProfileBox {
public:
    OpProfileBox(string name, vector<string> attributes)
        : name{move(name)}, attributes{move(attributes)} {}

    inline string getName() const { return name; }

    string getAttribute(uint32_t i) const;

    inline uint32_t getNumAttributes() const { return attributes.size(); }

    uint32_t getAttributeMaxLen() const;

private:
    string name;
    vector<string> attributes;
};

class OpProfileTree {
public:
    OpProfileTree(PhysicalOperator* opProfileBoxes, Profiler& profiler,
        unordered_map<uint32_t, shared_ptr<LogicalOperator>> physicalToLogicalOperatorMap);

    void prettyPrintToShell() const;

private:
    static void calculateNumRowsAndColsForOp(
        PhysicalOperator* op, uint32_t& numRows, uint32_t& numCols);

    uint32_t fillOpProfileBoxes(PhysicalOperator* op, uint32_t rowIdx, uint32_t colIdx,
        uint32_t& maxFieldWidth, Profiler& profiler,
        unordered_map<uint32_t, shared_ptr<LogicalOperator>>& physicalToLogicalOperatorMap);

    void printOpProfileBoxUpperFrame(uint32_t rowIdx, ostringstream& oss) const;

    void printOpProfileBoxes(uint32_t rowIdx, ostringstream& oss) const;

    void printOpProfileBoxLowerFrame(uint32_t rowIdx, ostringstream& oss) const;

    void prettyPrintPlanTitle() const;

    static string genHorizLine(uint32_t len);

    inline void validateRowIdxAndColIdx(uint32_t rowIdx, uint32_t colIdx) const {
        assert(0 <= rowIdx && rowIdx < opProfileBoxes.size() && 0 <= colIdx &&
               colIdx < opProfileBoxes[rowIdx].size());
    }

    void insertOpProfileBox(
        uint32_t rowIdx, uint32_t colIdx, unique_ptr<OpProfileBox> opProfileBox);

    OpProfileBox* getOpProfileBox(uint32_t rowIdx, uint32_t colIdx) const;

    bool hasOpProfileBox(uint32_t rowIdx, uint32_t colIdx) const {
        return 0 <= rowIdx && rowIdx < opProfileBoxes.size() && 0 <= colIdx &&
               colIdx < opProfileBoxes[rowIdx].size() && getOpProfileBox(rowIdx, colIdx);
    }

    //! Returns true if there is a valid OpProfileBox on the upper left side of the OpProfileBox
    //! located at (rowIdx, colIdx).
    bool hasOpProfileBoxOnUpperLeft(uint32_t rowIdx, uint32_t colIdx) const;

    uint32_t calculateRowHeight(uint32_t rowIdx) const;

private:
    vector<vector<unique_ptr<OpProfileBox>>> opProfileBoxes;
    uint32_t opProfileBoxWidth;
    static constexpr uint32_t INDENT_WIDTH = 3u;
    static constexpr uint32_t BOX_FRAME_WIDTH = 1u;
};

class PlanPrinter {

public:
    PlanPrinter(unique_ptr<PhysicalPlan> physicalPlan,
        unordered_map<uint32_t, shared_ptr<LogicalOperator>> physicalToLogicalOperatorMap)
        : physicalPlan{move(physicalPlan)}, physicalToLogicalOperatorMap{
                                                move(physicalToLogicalOperatorMap)} {}

    inline nlohmann::json printPlanToJson(Profiler& profiler) {
        return toJson(physicalPlan->lastOperator.get(), profiler);
    }

    inline void printPlanToShell(Profiler& profiler) {
        OpProfileTree(physicalPlan->lastOperator.get(), profiler, physicalToLogicalOperatorMap)
            .prettyPrintToShell();
    }

    static string getOperatorName(PhysicalOperator* physicalOperator,
        unordered_map<uint32_t, shared_ptr<LogicalOperator>>& physicalToLogicalOperatorMap);

private:
    nlohmann::json toJson(PhysicalOperator* physicalOperator, Profiler& profiler);

private:
    unique_ptr<PhysicalPlan> physicalPlan;
    unordered_map<uint32_t, shared_ptr<LogicalOperator>> physicalToLogicalOperatorMap;
};

} // namespace main
} // namespace graphflow
