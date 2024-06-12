#pragma once

#include "binder/bound_statement.h"
#include "function/copy/copy_function.h"

namespace kuzu {
namespace binder {

class BoundCopyTo : public BoundStatement {
public:
    BoundCopyTo(std::unique_ptr<function::CopyFuncBindData> bindData,
        function::CopyFunction copyFunc, std::unique_ptr<BoundStatement> query)
        : BoundStatement{common::StatementType::COPY_TO, BoundStatementResult::createEmptyResult()},
          bindData{std::move(bindData)}, copyFunc{std::move(copyFunc)}, query{std::move(query)} {}

    std::unique_ptr<function::CopyFuncBindData> getBindData() const { return bindData->copy(); }

    function::CopyFunction getCopyFunc() const { return copyFunc; }

    const BoundStatement* getRegularQuery() const { return query.get(); }

private:
    std::unique_ptr<function::CopyFuncBindData> bindData;
    function::CopyFunction copyFunc;
    std::unique_ptr<BoundStatement> query;
};

} // namespace binder
} // namespace kuzu
