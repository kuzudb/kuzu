#include "include/py_database.h"

void PyDatabase::initialize(py::handle& m) {
    py::class_<PyDatabase>(m, "database").def(py::init<const string&>());
}

PyDatabase::PyDatabase(const string& databasePath) {
    database = make_unique<Database>(DatabaseConfig(databasePath), SystemConfig());
}
