#pragma once

#include "binder/bound_statement.h"
#include "function/export/export_function.h"

namespace kuzu {
namespace binder {

class BoundCopyTo : public BoundStatement {
public:
    BoundCopyTo(std::unique_ptr<function::ExportFuncBindData> bindData,
        function::ExportFunction exportFunc, std::unique_ptr<BoundStatement> query)
        : BoundStatement{common::StatementType::COPY_TO, BoundStatementResult::createEmptyResult()},
          bindData{std::move(bindData)}, exportFunc{std::move(exportFunc)},
          query{std::move(query)} {}

    std::unique_ptr<function::ExportFuncBindData> getBindData() const { return bindData->copy(); }

    function::ExportFunction getExportFunc() const { return exportFunc; }

    const BoundStatement* getRegularQuery() const { return query.get(); }

private:
    std::unique_ptr<function::ExportFuncBindData> bindData;
    function::ExportFunction exportFunc;
    std::unique_ptr<BoundStatement> query;
};

} // namespace binder
} // namespace kuzu
