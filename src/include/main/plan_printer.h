#pragma once

#include <sstream>
#include <string>

#include "common/assert.h"
#include "common/profiler.h"
#include "json_fwd.hpp"
#include "kuzu_fwd.h"

namespace kuzu {
namespace main {

class OpProfileBox {
public:
    OpProfileBox(std::string opName, const std::string& paramsName,
        std::vector<std::string> attributes);

    inline std::string getOpName() const { return opName; }

    inline uint32_t getNumParams() const { return paramsNames.size(); }

    std::string getParamsName(uint32_t idx) const;

    std::string getAttribute(uint32_t idx) const;

    inline uint32_t getNumAttributes() const { return attributes.size(); }

    uint32_t getAttributeMaxLen() const;

private:
    std::string opName;
    std::vector<std::string> paramsNames;
    std::vector<std::string> attributes;
};

class OpProfileTree {
public:
    OpProfileTree(processor::PhysicalOperator* opProfileBoxes, common::Profiler& profiler);

    std::ostringstream printPlanToOstream() const;

private:
    static void calculateNumRowsAndColsForOp(processor::PhysicalOperator* op, uint32_t& numRows,
        uint32_t& numCols);

    uint32_t fillOpProfileBoxes(processor::PhysicalOperator* op, uint32_t rowIdx, uint32_t colIdx,
        uint32_t& maxFieldWidth, common::Profiler& profiler);

    void printOpProfileBoxUpperFrame(uint32_t rowIdx, std::ostringstream& oss) const;

    void printOpProfileBoxes(uint32_t rowIdx, std::ostringstream& oss) const;

    void printOpProfileBoxLowerFrame(uint32_t rowIdx, std::ostringstream& oss) const;

    void prettyPrintPlanTitle(std::ostringstream& oss) const;

    static std::string genHorizLine(uint32_t len);

    inline void validateRowIdxAndColIdx(uint32_t rowIdx, uint32_t colIdx) const {
        KU_ASSERT(rowIdx < opProfileBoxes.size() && colIdx < opProfileBoxes[rowIdx].size());
        (void)rowIdx;
        (void)colIdx;
    }

    void insertOpProfileBox(uint32_t rowIdx, uint32_t colIdx,
        std::unique_ptr<OpProfileBox> opProfileBox);

    OpProfileBox* getOpProfileBox(uint32_t rowIdx, uint32_t colIdx) const;

    bool hasOpProfileBox(uint32_t rowIdx, uint32_t colIdx) const {
        return rowIdx < opProfileBoxes.size() && colIdx < opProfileBoxes[rowIdx].size() &&
               getOpProfileBox(rowIdx, colIdx);
    }

    //! Returns true if there is a valid OpProfileBox on the upper left side of the OpProfileBox
    //! located at (rowIdx, colIdx).
    bool hasOpProfileBoxOnUpperLeft(uint32_t rowIdx, uint32_t colIdx) const;

    uint32_t calculateRowHeight(uint32_t rowIdx) const;

private:
    std::vector<std::vector<std::unique_ptr<OpProfileBox>>> opProfileBoxes;
    uint32_t opProfileBoxWidth;
    static constexpr uint32_t INDENT_WIDTH = 3u;
    static constexpr uint32_t BOX_FRAME_WIDTH = 1u;
};

class PlanPrinter {

public:
    PlanPrinter(processor::PhysicalPlan* physicalPlan, common::Profiler* profiler)
        : physicalPlan{physicalPlan}, profiler{profiler} {};

    nlohmann::json printPlanToJson();

    std::ostringstream printPlanToOstream();

    static std::string getOperatorName(processor::PhysicalOperator* physicalOperator);

    static std::string getOperatorParams(processor::PhysicalOperator* physicalOperator);

private:
    nlohmann::json toJson(processor::PhysicalOperator* physicalOperator,
        common::Profiler& profiler);

private:
    processor::PhysicalPlan* physicalPlan;
    common::Profiler* profiler;
};

} // namespace main
} // namespace kuzu
