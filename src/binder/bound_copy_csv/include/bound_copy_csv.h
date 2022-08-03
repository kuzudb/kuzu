#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "src/binder/bound_statement/include/bound_statement.h"
#include "src/common/types/include/node_id_t.h"

using namespace std;

namespace graphflow {
namespace binder {

class BoundCopyCSV : public BoundStatement {
public:
    BoundCopyCSV(string csvFileName, label_t labelID, bool isNodeLabel,
        unordered_map<string, char> parsingOptions)
        : BoundStatement{StatementType::COPY_CSV}, csvFileName{move(csvFileName)}, labelID{labelID},
          isNodeLabel{isNodeLabel}, parsingOptions{move(parsingOptions)} {}

    inline string getCSVFileName() const { return csvFileName; }
    inline label_t getLabelID() const { return labelID; }
    inline bool getIsNodeLabel() const { return isNodeLabel; }
    inline unordered_map<string, char> getParsingOptions() const { return parsingOptions; }

private:
    string csvFileName;
    label_t labelID;
    bool isNodeLabel;
    unordered_map<string, char> parsingOptions;
};

} // namespace binder
} // namespace graphflow
