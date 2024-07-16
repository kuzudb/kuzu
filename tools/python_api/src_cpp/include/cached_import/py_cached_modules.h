#pragma once

#include "py_cached_item.h"

namespace kuzu {

class DateTimeCachedItem : public PythonCachedItem {
public:
    DateTimeCachedItem()
        : PythonCachedItem("datetime"), date("date", this), datetime("datetime", this),
          timedelta("timedelta", this) {}

    PythonCachedItem date;
    PythonCachedItem datetime;
    PythonCachedItem timedelta;
};

class DecimalCachedItem : public PythonCachedItem {
public:
    DecimalCachedItem() : PythonCachedItem("decimal"), Decimal("Decimal", this) {}

    PythonCachedItem Decimal;
};

class ImportLibCachedItem : public PythonCachedItem {
    class UtilCachedItem : public PythonCachedItem {
    public:
        explicit UtilCachedItem(PythonCachedItem* parent)
            : PythonCachedItem{"util", parent}, find_spec{"find_spec", this} {}

        PythonCachedItem find_spec;
    };

public:
    ImportLibCachedItem() : PythonCachedItem("importlib"), util(this) {}

    UtilCachedItem util;
};

class InspectCachedItem : public PythonCachedItem {
public:
    InspectCachedItem()
        : PythonCachedItem("inspect"), currentframe("currentframe", this),
          signature("signature", this), _empty("_empty", this) {}

    PythonCachedItem currentframe;
    PythonCachedItem signature;
    PythonCachedItem _empty;
};

class NumpyMaCachedItem : public PythonCachedItem {
public:
    NumpyMaCachedItem() : PythonCachedItem("numpy.ma"), masked_array("masked_array", this) {}

    PythonCachedItem masked_array;
};

class PandasCachedItem : public PythonCachedItem {
    class SeriesCachedItem : public PythonCachedItem {
    public:
        explicit SeriesCachedItem(PythonCachedItem* parent)
            : PythonCachedItem("series", parent), Series("Series", this) {}

        PythonCachedItem Series;
    };

    class CoreCachedItem : public PythonCachedItem {
    public:
        explicit CoreCachedItem(PythonCachedItem* parent)
            : PythonCachedItem("core", parent), series(this) {}

        SeriesCachedItem series;
    };

    class DataFrameCachedItem : public PythonCachedItem {
    public:
        explicit DataFrameCachedItem(PythonCachedItem* parent)
            : PythonCachedItem("DataFrame", parent), from_dict("from_dict", this) {}

        PythonCachedItem from_dict;
    };

public:
    PandasCachedItem()
        : PythonCachedItem("pandas"), ArrowDtype("ArrowDtype", this), core(this), DataFrame(this),
          NA("NA", this), NaT("NaT", this) {}

    PythonCachedItem ArrowDtype;
    CoreCachedItem core;
    DataFrameCachedItem DataFrame;
    PythonCachedItem NA;
    PythonCachedItem NaT;
};

class PolarsCachedItem : public PythonCachedItem {
public:
    PolarsCachedItem() : PythonCachedItem("polars"), DataFrame("DataFrame", this) {}

    PythonCachedItem DataFrame;
};

class PyarrowCachedItem : public PythonCachedItem {
    class RecordBatchCachedItem : public PythonCachedItem {
    public:
        explicit RecordBatchCachedItem(PythonCachedItem* parent)
            : PythonCachedItem("RecordBatch", parent), _import_from_c("_import_from_c", this) {}

        PythonCachedItem _import_from_c;
    };

    class SchemaCachedItem : public PythonCachedItem {
    public:
        explicit SchemaCachedItem(PythonCachedItem* parent)
            : PythonCachedItem("Schema", parent), _import_from_c("_import_from_c", this) {}

        PythonCachedItem _import_from_c;
    };

    class TableCachedItem : public PythonCachedItem {
    public:
        explicit TableCachedItem(PythonCachedItem* parent)
            : PythonCachedItem("Table", parent), from_batches("from_batches", this),
              from_pandas("from_pandas", this) {}

        PythonCachedItem from_batches;
        PythonCachedItem from_pandas;
    };

    class LibCachedItem : public PythonCachedItem {
    public:
        explicit LibCachedItem(PythonCachedItem* parent)
            : PythonCachedItem("lib", parent), RecordBatch(this), Schema(this), Table(this) {}

        RecordBatchCachedItem RecordBatch;
        SchemaCachedItem Schema;
        TableCachedItem Table;
    };

public:
    PyarrowCachedItem() : PythonCachedItem("pyarrow"), lib(this) {}

    LibCachedItem lib;
};

class UUIDCachedItem : public PythonCachedItem {
public:
    UUIDCachedItem() : PythonCachedItem("uuid"), UUID("UUID", this) {}

    PythonCachedItem UUID;
};

} // namespace kuzu
