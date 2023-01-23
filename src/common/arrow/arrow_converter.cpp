#include "common/arrow/arrow_converter.h"

#include "common/arrow/arrow_row_batch.h"

namespace kuzu {
namespace common {

struct ArrowSchemaHolder {
    vector<ArrowSchema> children;
    vector<ArrowSchema*> childrenPtrs;
    vector<unique_ptr<char[]>> ownedTypeNames;
};

static void releaseArrowSchema(ArrowSchema* schema) {
    if (!schema || schema->release) {
        return;
    }
    schema->release = nullptr;
    auto holder = static_cast<ArrowSchemaHolder*>(schema->private_data);
    delete holder;
}

static void initializeChild(ArrowSchema& child, const string& name = "") {
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

static void setArrowFormat(ArrowSchema& child, const DataType& type) {
    switch (type.typeID) {
    case DataTypeID::BOOL: {
        child.format = "b";
    } break;
    case DataTypeID::INT64: {
        child.format = "l";
    } break;
    case DataTypeID::DOUBLE: {
        child.format = "g";
    } break;
    case DataTypeID::DATE: {
        child.format = "tdD";
    } break;
    case DataTypeID::TIMESTAMP: {
        child.format = "tsu:";
    } break;
    case DataTypeID::INTERVAL: {
        child.format = "tDm";
    } break;
    case DataTypeID::STRING: {
        child.format = "u";
    } break;
    default:
        throw InternalException("Unsupported Arrow type " + Types::dataTypeToString(type));
    }
}

void ArrowConverter::toArrowSchema(
    ArrowSchema* outSchema, std::vector<DataType>& types, std::vector<std::string>& names) {
    assert(outSchema && types.size() == names.size());
    auto rootHolder = make_unique<ArrowSchemaHolder>();

    auto columnCount = (int64_t)names.size();
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
        initializeChild(child, names[i]);
        setArrowFormat(child, types[i]);
    }

    outSchema->private_data = rootHolder.release();
    outSchema->release = releaseArrowSchema;
}

void ArrowConverter::toArrowArray(
    main::QueryResult& queryResult, ArrowArray* outArray, std::int64_t chunkSize) {
    auto types = queryResult.getColumnDataTypes();
    auto rowBatch = make_unique<ArrowRowBatch>(types, chunkSize);
    *outArray = rowBatch->append(queryResult, chunkSize);
}

} // namespace common
} // namespace kuzu
