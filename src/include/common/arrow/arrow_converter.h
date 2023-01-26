#pragma once

#include <string>
#include <vector>

#include "common/arrow/arrow.h"
#include "common/types/types.h"
#include "main/query_result.h"

struct ArrowSchema;

namespace kuzu {
namespace common {

struct ArrowSchemaHolder {
    vector<ArrowSchema> children;
    vector<ArrowSchema*> childrenPtrs;
    std::vector<std::vector<ArrowSchema>> nestedChildren;
    std::vector<std::vector<ArrowSchema*>> nestedChildrenPtr;
    vector<unique_ptr<char[]>> ownedTypeNames;
};

struct ArrowConverter {
public:
    static unique_ptr<ArrowSchema> toArrowSchema(
        const vector<unique_ptr<main::DataTypeInfo>>& typesInfo);
    static void toArrowArray(
        main::QueryResult& queryResult, ArrowArray* out_array, std::int64_t chunkSize);

private:
    static void initializeChild(ArrowSchema& child, const string& name = "");
    static void setArrowFormatForStruct(
        ArrowSchemaHolder& rootHolder, ArrowSchema& child, const main::DataTypeInfo& typeInfo);
    static void setArrowFormat(
        ArrowSchemaHolder& rootHolder, ArrowSchema& child, const main::DataTypeInfo& typeInfo);
};

} // namespace common
} // namespace kuzu
