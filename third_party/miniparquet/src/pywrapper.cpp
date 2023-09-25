#define PY_SSIZE_T_CLEAN
#include <Python.h>

// motherfucker
#undef error
#undef length

#include "miniparquet.h"

#include <cmath>
#include <iostream>

using namespace miniparquet;
using namespace std;

// surely they are joking
constexpr int64_t kJulianToUnixEpochDays = 2440588LL;
constexpr int64_t kMillisecondsInADay = 86400000LL;
constexpr int64_t kNanosecondsInADay = kMillisecondsInADay * 1000LL * 1000LL;

static int64_t impala_timestamp_to_nanoseconds(const Int96 &impala_timestamp) {
	int64_t days_since_epoch = impala_timestamp.value[2] - kJulianToUnixEpochDays;

	int64_t nanoseconds;
	memcpy(&nanoseconds, impala_timestamp.value, sizeof(nanoseconds));
	return days_since_epoch * kNanosecondsInADay + nanoseconds;
}

struct PythonWrapperObject {
	PythonWrapperObject() : obj(nullptr) {
	}
	PythonWrapperObject(PyObject *obj) : obj(obj) {
		if (!obj) {
			throw runtime_error("malloc failure");
		}
	}
	~PythonWrapperObject() {
		if (obj) {
			Py_DECREF(obj);
		}
	}
	PyObject *Release() {
		PyObject *res = obj;
		obj = nullptr;
		return res;
	}

	PyObject *obj;
};

static PyObject *miniparquet_read(PyObject *self, PyObject *args) {
	const char *fname;
	if (!PyArg_ParseTuple(args, "s", &fname)) {
		return NULL;
	}

	try {
		// parse the query and transform it into a set of statements
		ParquetFile f(fname);

		auto ncols = f.columns.size();
		auto nrows = f.nrow;

		PythonWrapperObject rdict(PyDict_New());
		auto pynames = unique_ptr<PythonWrapperObject[]>(new PythonWrapperObject[ncols]);
		auto pylists = unique_ptr<PythonWrapperObject[]>(new PythonWrapperObject[ncols]);

		for (size_t col_idx = 0; col_idx < ncols; col_idx++) {
			auto name = f.columns[col_idx]->name;
			pynames[col_idx].obj = PyUnicode_DecodeUTF8(name.c_str(), name.size(), nullptr);
			pylists[col_idx].obj = PyList_New(nrows);
			PyDict_SetItem(rdict.obj, pynames[col_idx].obj, pylists[col_idx].obj);
		}

		ResultChunk rc;
		ScanState s;

		f.initialize_result(rc);
		uint64_t dest_offset = 0;

		while (f.scan(s, rc)) {
			for (size_t col_idx = 0; col_idx < ncols; col_idx++) {
				auto &col = rc.cols[col_idx];
				for (uint64_t row_idx = 0; row_idx < rc.nrows; row_idx++) {
					uint64_t target_idx = dest_offset + row_idx;
					PyObject *current_item = nullptr;
					if (!col.defined.ptr[row_idx]) {
						// NULL
						Py_INCREF(Py_None);
						current_item = Py_None;
					} else {
						switch (f.columns[col_idx]->type) {
						case parquet::format::Type::BOOLEAN: {
							auto value = ((bool *)col.data.ptr)[row_idx];
							current_item = PyBool_FromLong(value);
							break;
						}
						case parquet::format::Type::INT32: {
							auto value = ((int32_t *)col.data.ptr)[row_idx];
							current_item = PyLong_FromLong(value);
							break;
						}
						case parquet::format::Type::INT64: {
							auto value = ((int64_t *)col.data.ptr)[row_idx];
							current_item = PyLong_FromLong(value);
							break;
						}
						case parquet::format::Type::DOUBLE: {
							auto value = ((double *)col.data.ptr)[row_idx];
							current_item = PyFloat_FromDouble(value);
							break;
						}
						case parquet::format::Type::FLOAT: {
							auto value = ((float *)col.data.ptr)[row_idx];
							current_item = PyFloat_FromDouble(value);
							break;
						}
						case parquet::format::Type::INT96: {
							auto nanoseconds =
							    impala_timestamp_to_nanoseconds(((Int96 *)col.data.ptr)[row_idx]) / 1000000000;
							current_item = PyLong_FromLong(nanoseconds);
							break;
						}
						case parquet::format::Type::FIXED_LEN_BYTE_ARRAY: { // oof, TODO clusterfuck
							auto &s_ele = f.columns[col_idx]->schema_element;
							if (!s_ele->__isset.converted_type) {
								throw runtime_error("Missing FLBA type");
							}
							switch (s_ele->converted_type) {
							case parquet::format::ConvertedType::DECIMAL: {

								// this is a giant clusterfuck
								auto type_len = s_ele->type_length;
								auto bytes = ((char **)col.data.ptr)[row_idx];
								int64_t val = 0;
								for (auto i = 0; i < type_len; i++) {
									val = val << ((type_len - i) * 8) | (uint8_t)bytes[i];
								}

								auto dbl = val / pow(10.0, s_ele->scale);
								current_item = PyFloat_FromDouble(dbl);
								break;
							}
							default:
								throw runtime_error("unknown FLBA type");
							}
							break;
						}
						case parquet::format::Type::BYTE_ARRAY: {
							auto value = ((char **)col.data.ptr)[row_idx];
							current_item = PyUnicode_DecodeUTF8(value, strlen(value), nullptr);
							break;
						}
						default: {
							throw runtime_error("unknown column type");
						}
						}
					}
					PyList_SetItem(pylists[col_idx].obj, target_idx, current_item);
				}
			}
			dest_offset += rc.nrows;
		}
		assert(dest_offset == nrows);
		return rdict.Release();
	} catch (std::exception &ex) {
		PyErr_SetString(PyExc_RuntimeError, ex.what());
		return NULL;
	}
}

static PyMethodDef parquet_methods[] = {
    {"read", miniparquet_read, METH_VARARGS, "Read a parquet file from disk."}, {NULL, NULL, 0, NULL} /* Sentinel */
};

static struct PyModuleDef miniparquetmodule = {PyModuleDef_HEAD_INIT, "miniparquet", /* name of module */
                                               nullptr, /* module documentation, may be NULL */
                                               -1,      /* size of per-interpreter state of the module,
                                                           or -1 if the module keeps state in global variables. */
                                               parquet_methods};

PyMODINIT_FUNC PyInit_miniparquet(void) {
	return PyModule_Create(&miniparquetmodule);
}
