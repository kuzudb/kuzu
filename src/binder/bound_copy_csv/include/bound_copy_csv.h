#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "src/binder/bound_statement/include/bound_statement.h"
#include "src/common/include/csv_reader/csv_reader.h"
#include "src/common/types/include/node_id_t.h"

using namespace std;

namespace graphflow {
namespace binder {

class BoundCopyCSV : public BoundStatement {
public:
    BoundCopyCSV(CSVDescription csvDescription, Label label)
        : BoundStatement{StatementType::COPY_CSV},
          csvDescription{move(csvDescription)}, label{move(label)} {}

    inline CSVDescription getCSVDescription() const { return csvDescription; }

    inline Label getLabel() const { return label; }

private:
    CSVDescription csvDescription;
    Label label;
};

} // namespace binder
} // namespace graphflow
