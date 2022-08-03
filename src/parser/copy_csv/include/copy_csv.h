#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "src/parser/include/statement.h"

namespace graphflow {
namespace parser {

using namespace std;

class CopyCSV : public Statement {
public:
    explicit CopyCSV(
        string csvFileName, string labelName, unordered_map<string, string> parsingOptions)
        : Statement{StatementType::COPY_CSV}, csvFileName{move(csvFileName)},
          labelName{move(labelName)}, parsingOptions{move(parsingOptions)} {}

    inline string getCSVFileName() const { return csvFileName; }
    inline string getLabelName() const { return labelName; }
    inline unordered_map<string, string> getParsingOptions() const { return parsingOptions; }

private:
    string csvFileName;
    string labelName;
    unordered_map<string, string> parsingOptions;
};

} // namespace parser
} // namespace graphflow
