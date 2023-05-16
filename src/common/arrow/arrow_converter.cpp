#include "common/arrow/arrow_converter.h"

#include "common/arrow/arrow_row_batch.h"

namespace kuzu {
namespace common {

static void releaseArrowSchema(ArrowSchema* schema) {
    if (!schema || schema->release) {
        return;
    }
    schema->release = nullptr;
    auto holder = static_cast<ArrowSchemaHolder*>(schema->private_data);
    delete holder;
}

void ArrowConverter::initializeChild(ArrowSchema& child, const std::string& name) {
    //! Child is cleaned up by parent
    child.private_data = nullptr;
    child.release = releaseArrowSchema;

    //! Store the child schema
    child.flags = ARROW_FLAG_NULLABLE;
    child.name = name.c_str();
    child.n_children = 0;
    child.children = nullptr;
    child.metadata = nullptr;
    child.dictionary = nullptr;
}

void ArrowConverter::setArrowFormatForStruct(
    ArrowSchemaHolder& rootHolder, ArrowSchema& child, const main::DataTypeInfo& typeInfo) {
    auto& childrenTypesInfo = typeInfo.childrenTypesInfo;
    child.format = "+s";
    child.name = typeInfo.name.c_str();
    child.n_children = (std::int64_t)childrenTypesInfo.size();
    rootHolder.nestedChildren.emplace_back();
    rootHolder.nestedChildren.back().resize(child.n_children);
    rootHolder.nestedChildrenPtr.emplace_back();
    rootHolder.nestedChildrenPtr.back().resize(child.n_children);
    for (auto i = 0u; i < child.n_children; i++) {
        rootHolder.nestedChildrenPtr.back()[i] = &rootHolder.nestedChildren.back()[i];
    }
    child.children = &rootHolder.nestedChildrenPtr.back()[0];
    for (auto i = 0u; i < child.n_children; i++) {
        initializeChild(*child.children[i]);
        auto structFieldName = childrenTypesInfo[i]->name;
        auto structFieldNameLength = structFieldName.length();
        std::unique_ptr<char[]> namePtr = std::make_unique<char[]>(structFieldNameLength + 1);
        std::memcpy(namePtr.get(), structFieldName.c_str(), structFieldNameLength);
        namePtr[structFieldNameLength] = '\0';
        rootHolder.ownedTypeNames.push_back(std::move(namePtr));
        child.children[i]->name = rootHolder.ownedTypeNames.back().get();
        setArrowFormat(rootHolder, *child.children[i], *childrenTypesInfo[i]);
    }
}

void ArrowConverter::setArrowFormat(
    ArrowSchemaHolder& rootHolder, ArrowSchema& child, const main::DataTypeInfo& typeInfo) {
    switch (typeInfo.typeID) {
    case LogicalTypeID::BOOL: {
        child.format = "b";
    } break;
    case LogicalTypeID::INT64: {
        child.format = "l";
    } break;
    case LogicalTypeID::INT32: {
        child.format = "i";
    } break;
    case LogicalTypeID::INT16: {
        child.format = "s";
    } break;
    case LogicalTypeID::DOUBLE: {
        child.format = "g";
    } break;
    case LogicalTypeID::DATE: {
        child.format = "tdD";
    } break;
    case LogicalTypeID::TIMESTAMP: {
        child.format = "tsu:";
    } break;
    case LogicalTypeID::INTERVAL: {
        child.format = "tDm";
    } break;
    case LogicalTypeID::STRING: {
        child.format = "u";
    } break;
    case LogicalTypeID::VAR_LIST: {
        child.format = "+l";
        child.n_children = 1;
        rootHolder.nestedChildren.emplace_back();
        rootHolder.nestedChildren.back().resize(1);
        rootHolder.nestedChildrenPtr.emplace_back();
        rootHolder.nestedChildrenPtr.back().push_back(&rootHolder.nestedChildren.back()[0]);
        initializeChild(rootHolder.nestedChildren.back()[0]);
        child.children = &rootHolder.nestedChildrenPtr.back()[0];
        child.children[0]->name = "l";
        setArrowFormat(rootHolder, **child.children, *typeInfo.childrenTypesInfo[0]);
    } break;
    case LogicalTypeID::INTERNAL_ID:
    case LogicalTypeID::NODE:
    case LogicalTypeID::REL: {
        setArrowFormatForStruct(rootHolder, child, typeInfo);
    } break;
    default:
        throw InternalException(
            "Unsupported Arrow type " + LogicalTypeUtils::dataTypeToString(typeInfo.typeID));
    }
}

std::unique_ptr<ArrowSchema> ArrowConverter::toArrowSchema(
    const std::vector<std::unique_ptr<main::DataTypeInfo>>& typesInfo) {
    auto outSchema = std::make_unique<ArrowSchema>();
    auto rootHolder = std::make_unique<ArrowSchemaHolder>();

    auto columnCount = (int64_t)typesInfo.size();
    rootHolder->children.resize(columnCount);
    rootHolder->childrenPtrs.resize(columnCount);
    for (auto i = 0u; i < columnCount; i++) {
        rootHolder->childrenPtrs[i] = &rootHolder->children[i];
    }
    outSchema->children = rootHolder->childrenPtrs.data();
    outSchema->n_children = columnCount;

    outSchema->format = "+s"; // struct apparently
    outSchema->flags = 0;
    outSchema->metadata = nullptr;
    outSchema->name = "kuzu_query_result";
    outSchema->dictionary = nullptr;

    for (auto i = 0u; i < columnCount; i++) {
        auto& child = rootHolder->children[i];
        initializeChild(child, typesInfo[i]->name);
        setArrowFormat(*rootHolder, child, *typesInfo[i]);
    }

    outSchema->private_data = rootHolder.release();
    outSchema->release = releaseArrowSchema;
    return std::move(outSchema);
}

void ArrowConverter::toArrowArray(
    main::QueryResult& queryResult, ArrowArray* outArray, std::int64_t chunkSize) {
    auto typesInfo = queryResult.getColumnTypesInfo();
    auto rowBatch = make_unique<ArrowRowBatch>(std::move(typesInfo), chunkSize);
    *outArray = rowBatch->append(queryResult, chunkSize);
}

} // namespace common
} // namespace kuzu
