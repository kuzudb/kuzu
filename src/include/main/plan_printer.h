#pragma once

#include <fstream>
#include <sstream>
#include <string>

#include "../common/profiler.h"
#include "../common/types/types.h"
#include "forward_declarations.h"

using namespace std;
using namespace kuzu::common;

namespace kuzu {
namespace main {

using namespace kuzu::processor;

class OpProfileBox {
public:
    OpProfileBox(string opName, string paramsName, vector<string> attributes);

    inline string getOpName() const { return opName; }

    inline uint32_t getNumParams() const { return paramsNames.size(); }

    string getParamsName(uint32_t idx) const;

    string getAttribute(uint32_t idx) const;

    inline uint32_t getNumAttributes() const { return attributes.size(); }

    uint32_t getAttributeMaxLen() const;

private:
    string opName;
    vector<string> paramsNames;
    vector<string> attributes;
};

class OpProfileTree {
public:
    OpProfileTree(PhysicalOperator* opProfileBoxes, Profiler& profiler);

    ostringstream printPlanToOstream() const;

private:
    static void calculateNumRowsAndColsForOp(
        PhysicalOperator* op, uint32_t& numRows, uint32_t& numCols);

    uint32_t fillOpProfileBoxes(PhysicalOperator* op, uint32_t rowIdx, uint32_t colIdx,
        uint32_t& maxFieldWidth, Profiler& profiler);

    void printOpProfileBoxUpperFrame(uint32_t rowIdx, ostringstream& oss) const;

    void printOpProfileBoxes(uint32_t rowIdx, ostringstream& oss) const;

    void printOpProfileBoxLowerFrame(uint32_t rowIdx, ostringstream& oss) const;

    void prettyPrintPlanTitle(ostringstream& oss) const;

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
    PlanPrinter(PhysicalPlan* physicalPlan, unique_ptr<Profiler> profiler)
        : physicalPlan{physicalPlan}, profiler{move(profiler)} {}

    unique_ptr<nlohmann::json> printPlanToJson();

    ostringstream printPlanToOstream();

    static string getOperatorName(PhysicalOperator* physicalOperator);

    static string getOperatorParams(PhysicalOperator* physicalOperator);

private:
    unique_ptr<nlohmann::json> toJson(PhysicalOperator* physicalOperator, Profiler& profiler);

private:
    PhysicalPlan* physicalPlan;
    unique_ptr<Profiler> profiler;
};

} // namespace main
} // namespace kuzu
