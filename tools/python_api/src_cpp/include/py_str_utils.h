#pragma once

#include "pybind_include.h"

struct PyStrUtil {
    static bool isPyUnicodeCompatibleAscii(py::handle& obj) {
        return PyUnicode_IS_COMPACT_ASCII(obj.ptr());
    }

    static bool isPyUnicodeCompact(PyCompactUnicodeObject* obj) {
        return PyUnicode_IS_COMPACT(obj);
    }

    static bool isPyUnicodeASCII(PyCompactUnicodeObject* obj) { return PyUnicode_IS_ASCII(obj); }

    static const char* getUnicodeStrData(py::handle& obj) {
        return reinterpret_cast<const char*>(PyUnicode_DATA(obj.ptr()));
    }

    static uint64_t getUnicodeStrLen(py::handle& obj) { return PyUnicode_GET_LENGTH(obj.ptr()); }

    static int getPyUnicodeKind(py::handle& obj) { return PyUnicode_KIND(obj.ptr()); }

    static Py_UCS1* PyUnicode1ByteData(py::handle& obj) { return PyUnicode_1BYTE_DATA(obj.ptr()); }

    static Py_UCS2* PyUnicode2ByteData(py::handle& obj) { return PyUnicode_2BYTE_DATA(obj.ptr()); }

    static Py_UCS4* PyUnicode4ByteData(py::handle& obj) { return PyUnicode_4BYTE_DATA(obj.ptr()); }
};
