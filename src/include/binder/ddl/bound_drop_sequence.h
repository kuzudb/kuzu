#pragma once

#include "binder/bound_statement.h"

namespace kuzu {
namespace binder {

class BoundDropSequence final : public BoundStatement {
public:
    BoundDropSequence(common::sequence_id_t sequenceID, std::string sequenceName)
        : BoundStatement{common::StatementType::DROP_SEQUENCE,
              BoundStatementResult::createSingleStringColumnResult()},
          sequenceID{sequenceID}, sequenceName{std::move(sequenceName)} {}

    inline common::sequence_id_t getSequenceID() const { return sequenceID; }
    inline std::string getSequenceName() const { return sequenceName; }

private:
    common::sequence_id_t sequenceID;
    std::string sequenceName;
};

} // namespace binder
} // namespace kuzu
