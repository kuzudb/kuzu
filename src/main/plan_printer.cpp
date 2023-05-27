#include "main/plan_printer.h"

#include <sstream>

#include "json.hpp"
#include "processor/physical_plan.h"

using namespace kuzu::common;
using namespace kuzu::processor;

namespace kuzu {
namespace main {

OpProfileBox::OpProfileBox(
    std::string opName, std::string paramsName, std::vector<std::string> attributes)
    : opName{std::move(opName)}, attributes{std::move(attributes)} {
    std::stringstream paramsStream{paramsName};
    std::string paramStr = "";
    std::string subStr;
    bool subParam = false;
    // This loop splits the parameters by commas, while not
    // splitting up parameters that are operators.
    while (paramsStream.good()) {
        getline(paramsStream, subStr, ',');
        if (subStr.find('(') != std::string::npos && subStr.find(')') == std::string::npos) {
            paramStr = subStr;
            subParam = true;
            continue;
        } else if (subParam && subStr.find(')') == std::string::npos) {
            paramStr += "," + subStr;
            continue;
        } else if (subParam) {
            subStr = paramStr + ")";
            paramStr = "";
            subParam = false;
        }
        // This if statement discards any strings that are completely whitespace.
        if (subStr.find_first_not_of(" \t\n\v\f\r") != std::string::npos) {
            paramsNames.push_back(subStr);
        }
    }
}

uint32_t OpProfileBox::getAttributeMaxLen() const {
    auto maxAttributeLen = opName.length();
    for (auto& param : paramsNames) {
        maxAttributeLen = std::max(param.length(), maxAttributeLen);
    }
    for (auto& attribute : attributes) {
        maxAttributeLen = std::max(attribute.length(), maxAttributeLen);
    }
    return maxAttributeLen;
}

std::string OpProfileBox::getParamsName(uint32_t idx) const {
    assert(idx < paramsNames.size());
    return paramsNames[idx];
}

std::string OpProfileBox::getAttribute(uint32_t idx) const {
    assert(idx < attributes.size());
    return attributes[idx];
}

OpProfileTree::OpProfileTree(PhysicalOperator* op, Profiler& profiler) {
    auto numRows = 0u, numCols = 0u;
    calculateNumRowsAndColsForOp(op, numRows, numCols);
    opProfileBoxes.resize(numRows);
    for_each(opProfileBoxes.begin(), opProfileBoxes.end(),
        [numCols](std::vector<std::unique_ptr<OpProfileBox>>& profileBoxes) {
            profileBoxes.resize(numCols);
        });
    auto maxFieldWidth = 0u;
    fillOpProfileBoxes(op, 0 /* rowIdx */, 0 /* colIdx */, maxFieldWidth, profiler);
    // The width of each profileBox = fieldWidth + leftIndentWidth + boxLeftFrameWidth +
    // rightIndentWidth + boxRightFrameWidth;
    this->opProfileBoxWidth = maxFieldWidth + 2 * (INDENT_WIDTH + BOX_FRAME_WIDTH);
}

void printSpaceIfNecessary(uint32_t idx, std::ostringstream& oss) {
    if (idx > 0) {
        oss << " ";
    }
}

std::ostringstream OpProfileTree::printPlanToOstream() const {
    std::ostringstream oss;
    prettyPrintPlanTitle(oss);
    for (auto i = 0u; i < opProfileBoxes.size(); i++) {
        printOpProfileBoxUpperFrame(i, oss);
        printOpProfileBoxes(i, oss);
        printOpProfileBoxLowerFrame(i, oss);
    }
    return oss;
}

void OpProfileTree::calculateNumRowsAndColsForOp(
    PhysicalOperator* op, uint32_t& numRows, uint32_t& numCols) {
    if (!op->getNumChildren()) {
        numRows = 1;
        numCols = 1;
        return;
    }

    for (auto i = 0u; i < op->getNumChildren(); i++) {
        auto numRowsInChild = 0u, numColsInChild = 0u;
        calculateNumRowsAndColsForOp(op->getChild(i), numRowsInChild, numColsInChild);
        numCols += numColsInChild;
        numRows = std::max(numRowsInChild, numRows);
    }
    numRows++;
}

uint32_t OpProfileTree::fillOpProfileBoxes(PhysicalOperator* op, uint32_t rowIdx, uint32_t colIdx,
    uint32_t& maxFieldWidth, Profiler& profiler) {
    auto opProfileBox = std::make_unique<OpProfileBox>(PlanPrinter::getOperatorName(op),
        PlanPrinter::getOperatorParams(op), op->getProfilerAttributes(profiler));
    maxFieldWidth = std::max(opProfileBox->getAttributeMaxLen(), maxFieldWidth);
    insertOpProfileBox(rowIdx, colIdx, std::move(opProfileBox));
    if (!op->getNumChildren()) {
        return 1;
    }

    uint32_t colOffset = 0;
    for (auto i = 0u; i < op->getNumChildren(); i++) {
        colOffset += fillOpProfileBoxes(
            op->getChild(i), rowIdx + 1, colIdx + colOffset, maxFieldWidth, profiler);
    }
    return colOffset;
}

void OpProfileTree::printOpProfileBoxUpperFrame(uint32_t rowIdx, std::ostringstream& oss) const {
    for (auto i = 0u; i < opProfileBoxes[rowIdx].size(); i++) {
        printSpaceIfNecessary(i, oss);
        if (getOpProfileBox(rowIdx, i)) {
            // If the current box has a parent, we need to put a "┴" in the  box upper frame to
            // connect to its parent.
            if (hasOpProfileBoxOnUpperLeft(rowIdx, i)) {
                auto leftFrameLength = (opProfileBoxWidth - 2 * BOX_FRAME_WIDTH - 1) / 2;
                oss << "┌" << genHorizLine(leftFrameLength) << "┴"
                    << genHorizLine(opProfileBoxWidth - 2 * BOX_FRAME_WIDTH - 1 - leftFrameLength)
                    << "┐";
            } else {
                oss << "┌" << genHorizLine(opProfileBoxWidth - 2 * BOX_FRAME_WIDTH) << "┐";
            }
        } else {
            oss << std::string(opProfileBoxWidth, ' ');
        }
    }
    oss << std::endl;
}

void OpProfileTree::printOpProfileBoxes(uint32_t rowIdx, std::ostringstream& oss) const {
    auto height = calculateRowHeight(rowIdx);
    auto halfWayPoint = height / 2;
    uint32_t offset = 0;
    for (auto i = 0u; i < height; i++) {
        for (auto j = 0u; j < opProfileBoxes[rowIdx].size(); j++) {
            auto opProfileBox = getOpProfileBox(rowIdx, j);
            if (opProfileBox &&
                i < 2 * (opProfileBox->getNumAttributes() + 1) + opProfileBox->getNumParams()) {
                printSpaceIfNecessary(j, oss);
                std::string textToPrint;
                unsigned int numParams = opProfileBox->getNumParams();
                if (i == 0) {
                    textToPrint = opProfileBox->getOpName();
                } else if (i == 1) {
                    textToPrint = std::string(opProfileBoxWidth - (1 + INDENT_WIDTH) * 2, '-');
                } else if (i <= numParams + 1) {
                    textToPrint = opProfileBox->getParamsName(i - 2);
                } else if ((i - numParams - 1) % 2) {
                    textToPrint = std::string(opProfileBoxWidth - (1 + INDENT_WIDTH) * 2, '-');
                } else {
                    textToPrint = opProfileBox->getAttribute((i - numParams - 1) / 2 - 1);
                }
                auto numLeftSpaces =
                    (opProfileBoxWidth - (1 + INDENT_WIDTH) * 2 - textToPrint.length()) / 2;
                auto numRightSpace = opProfileBoxWidth - (1 + INDENT_WIDTH) * 2 -
                                     textToPrint.length() - numLeftSpaces;
                oss << "│" << std::string(INDENT_WIDTH + numLeftSpaces, ' ') << textToPrint
                    << std::string(INDENT_WIDTH + numRightSpace, ' ') << "│";
            } else if (opProfileBox) {
                // If we have printed out all the attributes in the current opProfileBox, print
                // empty spaces as placeholders.
                printSpaceIfNecessary(j, oss);
                oss << "│" << std::string(opProfileBoxWidth - 2, ' ') << "│";
            } else {
                if (hasOpProfileBox(rowIdx + 1, j) && i >= halfWayPoint) {
                    auto leftHorizLineSize = (opProfileBoxWidth - 1) / 2;
                    if (i == halfWayPoint) {
                        oss << genHorizLine(leftHorizLineSize + 1);
                        if (hasOpProfileBox(rowIdx + 1, j + 1) && !hasOpProfileBox(rowIdx, j + 1)) {
                            oss << "┬" << genHorizLine(opProfileBoxWidth - 1 - leftHorizLineSize);
                        } else {
                            oss << "┐"
                                << std::string(opProfileBoxWidth - 1 - leftHorizLineSize, ' ');
                        }
                    } else if (i > halfWayPoint) {
                        printSpaceIfNecessary(j, oss);
                        oss << std::string(leftHorizLineSize, ' ') << "│"
                            << std::string(opProfileBoxWidth - 1 - leftHorizLineSize, ' ');
                    }
                } else if (hasOpProfileBox(rowIdx + 1, j + 1) && !hasOpProfileBox(rowIdx, j + 1) &&
                           i == halfWayPoint) {
                    oss << genHorizLine(opProfileBoxWidth + 1);
                    offset = offset == 0 ? 1 : 0;
                } else {
                    printSpaceIfNecessary(j, oss);
                    oss << std::string(opProfileBoxWidth, ' ');
                }
            }
        }
        oss << std::endl;
    }
}

void OpProfileTree::printOpProfileBoxLowerFrame(uint32_t rowIdx, std::ostringstream& oss) const {
    for (auto i = 0u; i < opProfileBoxes[rowIdx].size(); i++) {
        if (getOpProfileBox(rowIdx, i)) {
            printSpaceIfNecessary(i, oss);
            // If the current opProfileBox has a child, we need to print out a connector to it.
            if (hasOpProfileBox(rowIdx + 1, i)) {
                auto leftFrameLength = (opProfileBoxWidth - 2 * BOX_FRAME_WIDTH - 1) / 2;
                oss << "└" << genHorizLine(leftFrameLength) << "┬"
                    << genHorizLine(opProfileBoxWidth - 2 * BOX_FRAME_WIDTH - 1 - leftFrameLength)
                    << "┘";
            } else {
                oss << "└" << genHorizLine(opProfileBoxWidth - 2) << "┘";
            }
        } else if (hasOpProfileBox(rowIdx + 1, i)) {
            // If there is a opProfileBox at the bottom, we need to print out a vertical line to
            // connect it.
            auto leftFrameLength = (opProfileBoxWidth - 1) / 2;
            printSpaceIfNecessary(i, oss);
            oss << std::string(leftFrameLength, ' ') << "│"
                << std::string(opProfileBoxWidth - leftFrameLength - 1, ' ');
        } else {
            printSpaceIfNecessary(i, oss);
            oss << std::string(opProfileBoxWidth, ' ');
        }
    }
    oss << std::endl;
}

void OpProfileTree::prettyPrintPlanTitle(std::ostringstream& oss) const {
    const std::string physicalPlan = "Physical Plan";
    oss << "┌" << genHorizLine(opProfileBoxWidth - 2) << "┐" << std::endl;
    oss << "│┌" << genHorizLine(opProfileBoxWidth - 4) << "┐│" << std::endl;
    auto numLeftSpaces = (opProfileBoxWidth - physicalPlan.length() - 2 * (2 + INDENT_WIDTH)) / 2;
    auto numRightSpaces =
        opProfileBoxWidth - physicalPlan.length() - 2 * (2 + INDENT_WIDTH) - numLeftSpaces;
    oss << "││" << std::string(INDENT_WIDTH + numLeftSpaces, ' ') << physicalPlan
        << std::string(INDENT_WIDTH + numRightSpaces, ' ') << "││" << std::endl;
    oss << "│└" << genHorizLine(opProfileBoxWidth - 4) << "┘│" << std::endl;
    oss << "└" << genHorizLine(opProfileBoxWidth - 2) << "┘" << std::endl;
}

std::string OpProfileTree::genHorizLine(uint32_t len) {
    std::ostringstream tableFrame;
    for (auto i = 0u; i < len; i++) {
        tableFrame << "─";
    }
    return tableFrame.str();
}

void OpProfileTree::insertOpProfileBox(
    uint32_t rowIdx, uint32_t colIdx, std::unique_ptr<OpProfileBox> opProfileBox) {
    validateRowIdxAndColIdx(rowIdx, colIdx);
    opProfileBoxes[rowIdx][colIdx] = std::move(opProfileBox);
}

OpProfileBox* OpProfileTree::getOpProfileBox(uint32_t rowIdx, uint32_t colIdx) const {
    validateRowIdxAndColIdx(rowIdx, colIdx);
    return opProfileBoxes[rowIdx][colIdx].get();
}

bool OpProfileTree::hasOpProfileBoxOnUpperLeft(uint32_t rowIdx, uint32_t colIdx) const {
    validateRowIdxAndColIdx(rowIdx, colIdx);
    for (auto i = 0u; i <= colIdx; i++) {
        if (hasOpProfileBox(rowIdx - 1, i)) {
            return true;
        }
    }
    return false;
}

uint32_t OpProfileTree::calculateRowHeight(uint32_t rowIdx) const {
    validateRowIdxAndColIdx(rowIdx, 0 /* colIdx */);
    auto height = 0u;
    for (auto i = 0u; i < opProfileBoxes[rowIdx].size(); i++) {
        auto opProfileBox = getOpProfileBox(rowIdx, i);
        if (opProfileBox) {
            height = std::max(
                height, 2 * opProfileBox->getNumAttributes() + opProfileBox->getNumParams());
        }
    }
    return height + 2;
}

PlanPrinter::PlanPrinter(
    processor::PhysicalPlan* physicalPlan, std::unique_ptr<common::Profiler> profiler)
    : physicalPlan{physicalPlan}, profiler{std::move(profiler)} {}

nlohmann::json PlanPrinter::printPlanToJson() {
    return toJson(physicalPlan->lastOperator.get(), *profiler);
}

std::ostringstream PlanPrinter::printPlanToOstream() {
    return OpProfileTree(physicalPlan->lastOperator.get(), *profiler).printPlanToOstream();
}

std::string PlanPrinter::getOperatorName(processor::PhysicalOperator* physicalOperator) {
    return processor::PhysicalOperatorUtils::operatorTypeToString(
        physicalOperator->getOperatorType());
}

std::string PlanPrinter::getOperatorParams(processor::PhysicalOperator* physicalOperator) {
    return physicalOperator->getParamsString();
}

nlohmann::json PlanPrinter::toJson(PhysicalOperator* physicalOperator, Profiler& profiler_) {
    auto json = nlohmann::json();
    json["Name"] = getOperatorName(physicalOperator);
    if (profiler_.enabled) {
        for (auto& [key, val] : physicalOperator->getProfilerKeyValAttributes(profiler_)) {
            json[key] = val;
        }
    }
    for (auto i = 0u; i < physicalOperator->getNumChildren(); ++i) {
        json["Child" + std::to_string(i)] = toJson(physicalOperator->getChild(i), profiler_);
    }
    return json;
}

} // namespace main
} // namespace kuzu
