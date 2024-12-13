#pragma once

#include "common/cast.h"
#include "common/enums/statement_type.h"

namespace kuzu {
namespace parser {

class Statement {
public:
    explicit Statement(common::StatementType statementType)
        : statementType{statementType}, skipResult_{false} {}

    virtual ~Statement() = default;

    common::StatementType getStatementType() const { return statementType; }
    void setToSkipResult() { skipResult_ = true; }
    bool skipResult() const { return skipResult_; }

    bool requireTx() const {
        switch (statementType) {
        case common::StatementType::TRANSACTION:
            return false;
        default:
            return true;
        }
    }

    template<class TARGET>
    TARGET& cast() {
        return common::ku_dynamic_cast<TARGET&>(*this);
    }
    template<class TARGET>
    const TARGET& constCast() const {
        return common::ku_dynamic_cast<const TARGET&>(*this);
    }
    template<class TARGET>
    const TARGET* constPtrCast() const {
        return common::ku_dynamic_cast<const TARGET*>(this);
    }

private:
    common::StatementType statementType;
    bool skipResult_;
};

} // namespace parser
} // namespace kuzu
