use crate::ffi::ffi;
use crate::logical_type::LogicalType;
use std::cmp::Ordering;
use std::convert::{TryFrom, TryInto};
use std::fmt;

pub enum ConversionError {
    /// Kuzu's internal date as the number of days since 1970-01-01
    Date(i32),
    /// Kuzu's internal timestamp as the number of microseconds since 1970-01-01
    Timestamp(i64),
}

impl std::fmt::Display for ConversionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::fmt::Debug for ConversionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use ConversionError::*;
        match self {
            Date(days) => write!(f, "Could not convert Kuzu date offset of UNIX_EPOCH + {days} days to time::Date"),
            Timestamp(us) => write!(f, "Could not convert Kuzu timestamp offset of UNIX_EPOCH + {us} microseconds to time::OffsetDateTime"),
        }
    }
}

impl std::error::Error for ConversionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}

/// NodeVal represents a node in the graph and stores the nodeID, label and properties of that
/// node.
#[derive(Clone, Debug, PartialEq)]
pub struct NodeVal {
    id: InternalID,
    label: String,
    properties: Vec<(String, Value)>,
}

impl NodeVal {
    pub fn new(id: InternalID, label: String) -> Self {
        NodeVal {
            id,
            label,
            properties: vec![],
        }
    }

    pub fn get_node_id(&self) -> &InternalID {
        &self.id
    }

    pub fn get_label_name(&self) -> &String {
        &self.label
    }

    /// Adds a property with the given key/value pair to the NodeVal
    /// # Arguments
    /// * `key`: The name of the property
    /// * `value`: The value of the property
    pub fn add_property(&mut self, key: String, value: Value) {
        self.properties.push((key, value));
    }

    /// Returns all properties of the NodeVal
    pub fn get_properties(&self) -> &Vec<(String, Value)> {
        &self.properties
    }
}

fn properties_display(
    f: &mut fmt::Formatter<'_>,
    properties: &Vec<(String, Value)>,
) -> fmt::Result {
    write!(f, "{{")?;
    for (index, (name, value)) in properties.iter().enumerate() {
        write!(f, "{}:{}", name, value)?;
        if index < properties.len() - 1 {
            write!(f, ",")?;
        }
    }
    write!(f, "}}")
}

impl std::fmt::Display for NodeVal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(label:{}, {}, ", self.label, self.id)?;
        properties_display(f, &self.properties)?;
        write!(f, ")")
    }
}

/// RelVal represents a relationship in the graph and stores the relID, src/dst nodes and properties of that
/// rel
#[derive(Clone, Debug, PartialEq)]
pub struct RelVal {
    src_node: InternalID,
    dst_node: InternalID,
    label: String,
    properties: Vec<(String, Value)>,
}

impl RelVal {
    pub fn new(src_node: InternalID, dst_node: InternalID, label: String) -> Self {
        RelVal {
            src_node,
            dst_node,
            label,
            properties: vec![],
        }
    }

    pub fn get_src_node(&self) -> &InternalID {
        &self.src_node
    }
    pub fn get_dst_node(&self) -> &InternalID {
        &self.dst_node
    }

    pub fn get_label_name(&self) -> &String {
        &self.label
    }

    /// Adds a property with the given key/value pair to the NodeVal
    /// # Arguments
    /// * `key`: The name of the property
    /// * `value`: The value of the property
    pub fn add_property(&mut self, key: String, value: Value) {
        self.properties.push((key, value));
    }

    /// Returns all properties of the RelVal
    pub fn get_properties(&self) -> &Vec<(String, Value)> {
        &self.properties
    }
}

impl std::fmt::Display for RelVal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({})-[label:{}, ", self.src_node, self.label)?;
        properties_display(f, &self.properties)?;
        write!(f, "]->({})", self.dst_node)
    }
}

/// Stores the table_id and offset of a node/rel.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InternalID {
    pub offset: u64,
    pub table_id: u64,
}

impl std::fmt::Display for InternalID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.table_id, self.offset)
    }
}

impl PartialOrd for InternalID {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for InternalID {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.table_id == other.table_id {
            self.offset.cmp(&other.offset)
        } else {
            self.table_id.cmp(&other.table_id)
        }
    }
}

/// Data types supported by Kùzu
///
/// Also see <https://kuzudb.com/docs/cypher/data-types/overview.html>
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null(LogicalType),
    Bool(bool),
    Int64(i64),
    Int32(i32),
    Int16(i16),
    Double(f64),
    Float(f32),
    /// Stored internally as the number of days since 1970-01-01 as a 32-bit signed integer, which
    /// allows for a wider range of dates to be stored than can be represented by time::Date
    ///
    /// <https://kuzudb.com/docs/cypher/data-types/date.html>
    Date(time::Date),
    /// May be signed or unsigned.
    ///
    /// Nanosecond precision of time::Duration (if available) will not be preserved when passed to
    /// queries, and results will always have at most microsecond precision.
    ///
    /// <https://kuzudb.com/docs/cypher/data-types/interval.html>
    Interval(time::Duration),
    /// Stored internally as the number of microseconds since 1970-01-01
    /// Nanosecond precision of SystemTime (if available) will not be preserved when used.
    ///
    /// <https://kuzudb.com/docs/cypher/data-types/timestamp.html>
    Timestamp(time::OffsetDateTime),
    InternalID(InternalID),
    /// <https://kuzudb.com/docs/cypher/data-types/string.html>
    String(String),
    // TODO: Enforce type of contents
    // LogicalType is necessary so that we can pass the correct type to the C++ API if the list is empty.
    /// These must contain elements which are all the given type.
    /// <https://kuzudb.com/docs/cypher/data-types/list.html>
    VarList(LogicalType, Vec<Value>),
    /// These must contain elements which are all the same type.
    /// <https://kuzudb.com/docs/cypher/data-types/list.html>
    FixedList(LogicalType, Vec<Value>),
    /// <https://kuzudb.com/docs/cypher/data-types/struct.html>
    Struct(Vec<(String, Value)>),
    Node(NodeVal),
    Rel(RelVal),
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Bool(true) => write!(f, "True"),
            Value::Bool(false) => write!(f, "False"),
            Value::Int16(x) => write!(f, "{x}"),
            Value::Int32(x) => write!(f, "{x}"),
            Value::Int64(x) => write!(f, "{x}"),
            Value::Date(x) => write!(f, "{x}"),
            Value::String(x) => write!(f, "{x}"),
            Value::Null(_) => write!(f, ""),
            Value::VarList(_, x) | Value::FixedList(_, x) => {
                write!(f, "[")?;
                for (i, value) in x.iter().enumerate() {
                    write!(f, "{}", value)?;
                    if i != x.len() - 1 {
                        write!(f, ",")?;
                    }
                }
                write!(f, "]")
            }
            // Note: These don't match kuzu's toString, but we probably don't want them to
            Value::Interval(x) => write!(f, "{x}"),
            Value::Timestamp(x) => write!(f, "{x}"),
            Value::Float(x) => write!(f, "{x}"),
            Value::Double(x) => write!(f, "{x}"),
            Value::Struct(x) => {
                write!(f, "{{")?;
                for (i, (name, value)) in x.iter().enumerate() {
                    write!(f, "{}: {}", name, value)?;
                    if i != x.len() - 1 {
                        write!(f, ", ")?;
                    }
                }
                write!(f, "}}")
            }
            Value::Node(x) => write!(f, "{x}"),
            Value::Rel(x) => write!(f, "{x}"),
            Value::InternalID(x) => write!(f, "{x}"),
        }
    }
}

impl From<&Value> for LogicalType {
    fn from(value: &Value) -> Self {
        match value {
            Value::Bool(_) => LogicalType::Bool,
            Value::Int16(_) => LogicalType::Int16,
            Value::Int32(_) => LogicalType::Int32,
            Value::Int64(_) => LogicalType::Int64,
            Value::Float(_) => LogicalType::Float,
            Value::Double(_) => LogicalType::Double,
            Value::Date(_) => LogicalType::Date,
            Value::Interval(_) => LogicalType::Interval,
            Value::Timestamp(_) => LogicalType::Timestamp,
            Value::String(_) => LogicalType::String,
            Value::Null(x) => x.clone(),
            Value::VarList(x, _) => LogicalType::VarList {
                child_type: Box::new(x.clone()),
            },
            Value::FixedList(x, value) => LogicalType::FixedList {
                child_type: Box::new(x.clone()),
                num_elements: value.len() as u64,
            },
            Value::Struct(values) => LogicalType::Struct {
                fields: values
                    .iter()
                    .map(|(name, x)| {
                        let typ: LogicalType = x.into();
                        (name.clone(), typ)
                    })
                    .collect(),
            },
            Value::InternalID(_) => LogicalType::InternalID,
            Value::Node(_) => LogicalType::Node,
            Value::Rel(_) => LogicalType::Rel,
        }
    }
}

impl TryFrom<&ffi::Value> for Value {
    type Error = ConversionError;

    fn try_from(value: &ffi::Value) -> Result<Self, Self::Error> {
        use ffi::LogicalTypeID;
        if value.isNull() {
            return Ok(Value::Null(value.into()));
        }
        match ffi::value_get_data_type_id(value) {
            LogicalTypeID::ANY => unimplemented!(),
            LogicalTypeID::BOOL => Ok(Value::Bool(value.get_value_bool())),
            LogicalTypeID::INT16 => Ok(Value::Int16(value.get_value_i16())),
            LogicalTypeID::INT32 => Ok(Value::Int32(value.get_value_i32())),
            LogicalTypeID::INT64 => Ok(Value::Int64(value.get_value_i64())),
            LogicalTypeID::FLOAT => Ok(Value::Float(value.get_value_float())),
            LogicalTypeID::DOUBLE => Ok(Value::Double(value.get_value_double())),
            LogicalTypeID::STRING => Ok(Value::String(ffi::value_get_string(value))),
            LogicalTypeID::INTERVAL => Ok(Value::Interval(time::Duration::new(
                ffi::value_get_interval_secs(value),
                // Duration is constructed using nanoseconds, but kuzu stores microseconds
                ffi::value_get_interval_micros(value) * 1000,
            ))),
            LogicalTypeID::DATE => {
                let days = ffi::value_get_date_days(value);
                time::Date::from_calendar_date(1970, time::Month::January, 1)
                    .unwrap()
                    .checked_add(time::Duration::days(days as i64))
                    .map(Value::Date)
                    .ok_or(ConversionError::Date(days))
            }
            LogicalTypeID::TIMESTAMP => {
                let us = ffi::value_get_timestamp_micros(value);
                time::OffsetDateTime::UNIX_EPOCH
                    .checked_add(time::Duration::microseconds(us))
                    .map(Value::Timestamp)
                    .ok_or(ConversionError::Timestamp(us))
            }
            LogicalTypeID::VAR_LIST => {
                let list = ffi::value_get_list(value);
                let mut result = vec![];
                for index in 0..list.size() {
                    let value: Value = list.get(index).as_ref().unwrap().try_into()?;
                    result.push(value);
                }
                if let LogicalType::VarList { child_type } = value.into() {
                    Ok(Value::VarList(*child_type, result))
                } else {
                    unreachable!()
                }
            }
            LogicalTypeID::FIXED_LIST => {
                let list = ffi::value_get_list(value);
                let mut result = vec![];
                for index in 0..list.size() {
                    let value: Value = list.get(index).as_ref().unwrap().try_into()?;
                    result.push(value);
                }
                if let LogicalType::FixedList { child_type, .. } = value.into() {
                    Ok(Value::FixedList(*child_type, result))
                } else {
                    unreachable!()
                }
            }
            LogicalTypeID::STRUCT => {
                // Data is a list of field values in the value itself (same as list),
                // with the field names stored in the DataType
                let field_names = ffi::logical_type_get_struct_field_names(
                    ffi::value_get_data_type(value).as_ref().unwrap(),
                );
                let list = ffi::value_get_list(value);
                let mut result = vec![];
                for (name, index) in field_names.into_iter().zip(0..list.size()) {
                    let value: Value = list.get(index).as_ref().unwrap().try_into()?;
                    result.push((name, value));
                }
                Ok(Value::Struct(result))
            }
            LogicalTypeID::NODE => {
                let ffi_node_val = ffi::value_get_node_val(value);
                let id = ffi::node_value_get_node_id(ffi_node_val.as_ref().unwrap());
                let id = InternalID {
                    offset: id[0],
                    table_id: id[1],
                };
                let label = ffi::node_value_get_label_name(ffi_node_val.as_ref().unwrap());
                let mut node_val = NodeVal::new(id, label);
                let properties = ffi::node_value_get_properties(ffi_node_val.as_ref().unwrap());
                for i in 0..properties.size() {
                    node_val
                        .add_property(properties.get_name(i), properties.get_value(i).try_into()?);
                }
                Ok(Value::Node(node_val))
            }
            LogicalTypeID::REL => {
                let ffi_rel_val = ffi::value_get_rel_val(value);
                let src_node = ffi::rel_value_get_src_id(ffi_rel_val.as_ref().unwrap());
                let dst_node = ffi::rel_value_get_dst_id(ffi_rel_val.as_ref().unwrap());
                let src_node = InternalID {
                    offset: src_node[0],
                    table_id: src_node[1],
                };
                let dst_node = InternalID {
                    offset: dst_node[0],
                    table_id: dst_node[1],
                };
                let label = ffi::rel_value_get_label_name(ffi_rel_val.as_ref().unwrap());
                let mut rel_val = RelVal::new(src_node, dst_node, label);
                let properties = ffi::rel_value_get_properties(ffi_rel_val.as_ref().unwrap());
                for i in 0..properties.size() {
                    rel_val
                        .add_property(properties.get_name(i), properties.get_value(i).try_into()?);
                }
                Ok(Value::Rel(rel_val))
            }
            LogicalTypeID::INTERNAL_ID => {
                let internal_id = ffi::value_get_internal_id(value);
                Ok(Value::InternalID(InternalID {
                    offset: internal_id[0],
                    table_id: internal_id[1],
                }))
            }
            // Should be unreachable, as cxx will check that the LogicalTypeID enum matches the one
            // on the C++ side.
            x => panic!("Unsupported type {:?}", x),
        }
    }
}

impl TryInto<cxx::UniquePtr<ffi::Value>> for Value {
    // Errors should occur if:
    // - types are heterogeneous in lists
    type Error = crate::error::Error;

    fn try_into(self) -> Result<cxx::UniquePtr<ffi::Value>, Self::Error> {
        match self {
            Value::Null(typ) => Ok(ffi::create_value_null((&typ).into())),
            Value::Bool(value) => Ok(ffi::create_value_bool(value)),
            Value::Int16(value) => Ok(ffi::create_value_i16(value)),
            Value::Int32(value) => Ok(ffi::create_value_i32(value)),
            Value::Int64(value) => Ok(ffi::create_value_i64(value)),
            Value::Float(value) => Ok(ffi::create_value_float(value)),
            Value::Double(value) => Ok(ffi::create_value_double(value)),
            Value::String(value) => Ok(ffi::create_value_string(&value)),
            Value::Timestamp(value) => Ok(ffi::create_value_timestamp(
                // Convert to microseconds since 1970-01-01
                (value.unix_timestamp_nanos() / 1000) as i64,
            )),
            Value::Date(value) => Ok(ffi::create_value_date(
                // Convert to days since 1970-01-01
                (value - time::Date::from_ordinal_date(1970, 1).unwrap()).whole_days(),
            )),
            Value::Interval(value) => {
                use time::Duration;
                let mut interval = value;
                let months = interval.whole_days() / 30;
                interval -= Duration::days(months * 30);
                let days = interval.whole_days();
                interval -= Duration::days(days);
                let micros = interval.whole_microseconds() as i64;
                Ok(ffi::create_value_interval(
                    months as i32,
                    days as i32,
                    micros,
                ))
            }
            Value::VarList(typ, value) => {
                let mut builder = ffi::create_list();
                for elem in value {
                    builder.pin_mut().insert(elem.try_into()?);
                }
                Ok(ffi::get_list_value(
                    (&LogicalType::VarList {
                        child_type: Box::new(typ),
                    })
                        .into(),
                    builder,
                ))
            }
            Value::FixedList(typ, value) => {
                let mut builder = ffi::create_list();
                let len = value.len();
                for elem in value {
                    builder.pin_mut().insert(elem.try_into()?);
                }
                Ok(ffi::get_list_value(
                    (&LogicalType::FixedList {
                        child_type: Box::new(typ),
                        num_elements: len as u64,
                    })
                        .into(),
                    builder,
                ))
            }
            Value::Struct(value) => {
                let typ: LogicalType = LogicalType::Struct {
                    fields: value
                        .iter()
                        .map(|(name, value)| {
                            // Unwrap is safe since we already converted when inserting into the
                            // builder
                            (name.clone(), Into::<LogicalType>::into(value))
                        })
                        .collect(),
                };

                let mut builder = ffi::create_list();
                for (_, elem) in value {
                    builder.pin_mut().insert(elem.try_into()?);
                }

                Ok(ffi::get_list_value((&typ).into(), builder))
            }
            Value::InternalID(value) => {
                Ok(ffi::create_value_internal_id(value.offset, value.table_id))
            }
            Value::Node(value) => {
                let mut node = ffi::create_value_node(
                    Value::InternalID(value.id).try_into()?,
                    Value::String(value.label).try_into()?,
                );
                for (name, property) in value.properties {
                    ffi::value_add_property(node.pin_mut(), &name, property.try_into()?);
                }
                Ok(node)
            }
            Value::Rel(value) => {
                let mut rel = ffi::create_value_rel(
                    Value::InternalID(value.src_node).try_into()?,
                    Value::InternalID(value.dst_node).try_into()?,
                    Value::String(value.label).try_into()?,
                );
                for (name, property) in value.properties {
                    ffi::value_add_property(rel.pin_mut(), &name, property.try_into()?);
                }
                Ok(rel)
            }
        }
    }
}

impl From<i16> for Value {
    fn from(item: i16) -> Self {
        Value::Int16(item)
    }
}

impl From<i32> for Value {
    fn from(item: i32) -> Self {
        Value::Int32(item)
    }
}

impl From<i64> for Value {
    fn from(item: i64) -> Self {
        Value::Int64(item)
    }
}

impl From<f32> for Value {
    fn from(item: f32) -> Self {
        Value::Float(item)
    }
}

impl From<f64> for Value {
    fn from(item: f64) -> Self {
        Value::Double(item)
    }
}

impl From<String> for Value {
    fn from(item: String) -> Self {
        Value::String(item)
    }
}

impl From<&str> for Value {
    fn from(item: &str) -> Self {
        Value::String(item.to_string())
    }
}

#[cfg(test)]
mod tests {
    use crate::ffi::ffi;
    use crate::{
        connection::Connection,
        database::Database,
        logical_type::LogicalType,
        value::{InternalID, NodeVal, RelVal, Value},
    };
    use anyhow::Result;
    use std::collections::HashSet;
    use std::convert::TryInto;
    use std::iter::FromIterator;
    use time::macros::{date, datetime};

    // Note: Cargo runs tests in parallel by default, however kuzu does not support
    // working with multiple databases in parallel.
    // Tests can be run serially with `cargo test -- --test-threads=1` to work around this.

    macro_rules! type_tests {
        ($($name:ident: $value:expr,)*) => {
        $(
            #[test]
            /// Tests that the values are correctly converted into kuzu::common::Value and back
            fn $name() -> Result<()> {
                let rust_type: LogicalType = $value;
                let typ: cxx::UniquePtr<ffi::LogicalType> = (&rust_type).try_into()?;
                let new_rust_type: LogicalType = typ.as_ref().unwrap().try_into()?;
                assert_eq!(new_rust_type, rust_type);
                Ok(())
            }
        )*
        }
    }

    macro_rules! value_tests {
        ($($name:ident: $value:expr,)*) => {
        $(
            #[test]
            /// Tests that the values are correctly converted into kuzu::common::Value and back
            fn $name() -> Result<()> {
                let rust_value: Value = $value;
                let value: cxx::UniquePtr<ffi::Value> = rust_value.clone().try_into()?;
                let new_rust_value: Value = value.as_ref().unwrap().try_into()?;
                assert_eq!(new_rust_value, rust_value);
                Ok(())
            }
        )*
        }
    }

    macro_rules! display_tests {
        ($($name:ident: $value:expr,)*) => {
        $(
            #[test]
            /// Tests that the values are correctly converted into kuzu::common::Value and back
            fn $name() -> Result<()> {
                let rust_value: Value = $value;
                let value: cxx::UniquePtr<ffi::Value> = rust_value.clone().try_into()?;
                assert_eq!(ffi::value_to_string(value.as_ref().unwrap()), format!("{rust_value}"));
                Ok(())
            }
        )*
        }
    }

    macro_rules! database_tests {
        ($($name:ident: $value:expr, $decl:expr,)*) => {
        $(
            #[test]
            /// Tests that passing the values through the database returns what we put in
            fn $name() -> Result<()> {
                let temp_dir = tempdir::TempDir::new("example")?;
                let db = Database::new(temp_dir.path(), 0)?;
                let conn = Connection::new(&db)?;
                conn.query(&format!(
                    "CREATE NODE TABLE Person(name STRING, item {}, PRIMARY KEY(name));",
                    $decl,
                ))?;

                let mut add_person =
                    conn.prepare("CREATE (:Person {name: $name, item: $item});")?;
                conn.execute(
                    &mut add_person,
                    vec![("name", "Bob".into()), ("item", $value)],
                )?;
                let result = conn
                    .query("MATCH (a:Person) WHERE a.name = \"Bob\" RETURN a.item;")?
                    .next()
                    .unwrap();
                // TODO: Test equivalence to value constructed inside a a query
                assert_eq!(result[0], $value);
                temp_dir.close()?;
                Ok(())
            }
        )*
        }
    }

    type_tests! {
        convert_var_list_type: LogicalType::VarList { child_type: Box::new(LogicalType::String) },
        convert_fixed_list_type: LogicalType::FixedList { child_type: Box::new(LogicalType::Int64), num_elements: 3 },
        convert_int16_type: LogicalType::Int16,
        convert_int32_type: LogicalType::Int32,
        convert_int64_type: LogicalType::Int64,
        convert_float_type: LogicalType::Float,
        convert_double_type: LogicalType::Double,
        convert_timestamp_type: LogicalType::Timestamp,
        convert_date_type: LogicalType::Date,
        convert_interval_type: LogicalType::Interval,
        convert_string_type: LogicalType::String,
        convert_bool_type: LogicalType::Bool,
        convert_struct_type: LogicalType::Struct { fields: vec![("NAME".to_string(), LogicalType::String)]},
        convert_node_type: LogicalType::Node,
        convert_internal_id_type: LogicalType::InternalID,
        convert_rel_type: LogicalType::Rel,
    }

    value_tests! {
        convert_var_list: Value::VarList(LogicalType::String, vec!["Alice".into(), "Bob".into()]),
        convert_var_list_empty: Value::VarList(LogicalType::String, vec![]),
        convert_fixed_list: Value::FixedList(LogicalType::String, vec!["Alice".into(), "Bob".into()]),
        convert_int16: Value::Int16(1),
        convert_int32: Value::Int32(2),
        convert_int64: Value::Int64(3),
        convert_float: Value::Float(4.),
        convert_double: Value::Double(5.),
        convert_timestamp: Value::Timestamp(datetime!(2023-06-13 11:25:30 UTC)),
        convert_date: Value::Date(date!(2023-06-13)),
        convert_interval: Value::Interval(time::Duration::weeks(10)),
        convert_string: Value::String("Hello World".to_string()),
        convert_bool: Value::Bool(false),
        convert_null: Value::Null(LogicalType::VarList {
            child_type: Box::new(LogicalType::FixedList { child_type: Box::new(LogicalType::Int16), num_elements: 3 })
        }),
        convert_struct: Value::Struct(vec![("NAME".to_string(), "Alice".into()), ("AGE".to_string(), 25.into())]),
        convert_internal_id: Value::InternalID(InternalID { table_id: 0, offset: 0 }),
        convert_node: Value::Node(NodeVal::new(InternalID { table_id: 0, offset: 0 }, "Test Label".to_string())),
        convert_rel: Value::Rel(RelVal::new(InternalID { table_id: 0, offset: 0 }, InternalID { table_id: 1, offset: 0 }, "Test Label".to_string())),
    }

    display_tests! {
        display_var_list: Value::VarList(LogicalType::String, vec!["Alice".into(), "Bob".into()]),
        display_var_list_empty: Value::VarList(LogicalType::String, vec![]),
        display_fixed_list: Value::FixedList(LogicalType::String, vec!["Alice".into(), "Bob".into()]),
        display_int16: Value::Int16(1),
        display_int32: Value::Int32(2),
        display_int64: Value::Int64(3),
        // Float, doble, interval and timestamp have display differences which we probably don't want to
        // reconcile
        display_date: Value::Date(date!(2023-06-13)),
        display_string: Value::String("Hello World".to_string()),
        display_bool: Value::Bool(false),
        display_null: Value::Null(LogicalType::VarList {
            child_type: Box::new(LogicalType::FixedList { child_type: Box::new(LogicalType::Int16), num_elements: 3 })
        }),
        display_struct: Value::Struct(vec![("NAME".to_string(), "Alice".into()), ("AGE".to_string(), 25.into())]),
        display_internal_id: Value::InternalID(InternalID { table_id: 0, offset: 0 }),
        display_node: Value::Node(NodeVal::new(InternalID { table_id: 0, offset: 0 }, "Test Label".to_string())),
        display_rel: Value::Rel(RelVal::new(InternalID { table_id: 0, offset: 0 }, InternalID { table_id: 1, offset: 0 }, "Test Label".to_string())),
    }

    database_tests! {
        // Passing these values as arguments is not yet implemented in kuzu:
        // db_struct:
        //    Value::Struct(vec![("item".to_string(), "Knife".into()), ("count".to_string(), 1.into())]),
        //    "STRUCT(item STRING, count INT32)",
        // db_fixed_list: Value::FixedList(LogicalType::String, vec!["Alice".into(), "Bob".into()]), "STRING[2]",
        // db_null_string: Value::Null(LogicalType::String), "STRING",
        // db_null_int: Value::Null(LogicalType::Int64), "INT64",
        // db_null_list: Value::Null(LogicalType::VarList {
        //    child_type: Box::new(LogicalType::FixedList { child_type: Box::new(LogicalType::Int16), num_elements: 3 })
        // }), "INT16[3][]",
        // db_var_list_string: Value::VarList(LogicalType::String, vec!["Alice".into(), "Bob".into()]), "STRING[]",
        // db_var_list_int: Value::VarList(LogicalType::Int64, vec![0i64.into(), 1i64.into(), 2i64.into()]), "INT64[]",
        db_int16: Value::Int16(1), "INT16",
        db_int32: Value::Int32(2), "INT32",
        db_int64: Value::Int64(3), "INT64",
        db_float: Value::Float(4.), "FLOAT",
        db_double: Value::Double(5.), "DOUBLE",
        db_timestamp: Value::Timestamp(datetime!(2023-06-13 11:25:30 UTC)), "TIMESTAMP",
        db_date: Value::Date(date!(2023-06-13)), "DATE",
        db_interval: Value::Interval(time::Duration::weeks(200)), "INTERVAL",
        db_string: Value::String("Hello World".to_string()), "STRING",
        db_bool: Value::Bool(true), "BOOLEAN",
    }

    #[test]
    /// Tests that the list value is correctly constructed
    fn test_var_list_get() -> Result<()> {
        let temp_dir = tempdir::TempDir::new("example")?;
        let db = Database::new(temp_dir.path(), 0)?;
        let conn = Connection::new(&db)?;
        for result in conn.query("RETURN [\"Alice\", \"Bob\"] AS l;")? {
            assert_eq!(result.len(), 1);
            assert_eq!(
                result[0],
                Value::VarList(LogicalType::String, vec!["Alice".into(), "Bob".into(),])
            );
        }
        temp_dir.close()?;
        Ok(())
    }

    #[test]
    /// Test that the timestamp round-trips through kuzu's internal timestamp
    fn test_timestamp() -> Result<()> {
        let temp_dir = tempdir::TempDir::new("example")?;
        let db = Database::new(temp_dir.path(), 0)?;
        let conn = Connection::new(&db)?;
        conn.query(
            "CREATE NODE TABLE Person(name STRING, registerTime TIMESTAMP, PRIMARY KEY(name));",
        )?;
        conn.query(
            "CREATE (:Person {name: \"Alice\", registerTime: timestamp(\"2011-08-20 11:25:30\")});",
        )?;
        let mut add_person =
            conn.prepare("CREATE (:Person {name: $name, registerTime: $time});")?;
        let timestamp = datetime!(2011-08-20 11:25:30 UTC);
        conn.execute(
            &mut add_person,
            vec![
                ("name", "Bob".into()),
                ("time", Value::Timestamp(timestamp)),
            ],
        )?;
        let result: HashSet<String> = conn
            .query(
                "MATCH (a:Person) WHERE a.registerTime = timestamp(\"2011-08-20 11:25:30\") RETURN a.name;",
            )?
            .map(|x| match &x[0] {
                Value::String(x) => x.clone(),
                _ => unreachable!(),
            })
            .collect();
        assert_eq!(
            result,
            HashSet::from_iter(vec!["Alice".to_string(), "Bob".to_string()])
        );
        let mut result =
            conn.query("MATCH (a:Person) WHERE a.name = \"Bob\" RETURN a.registerTime;")?;
        let result: time::OffsetDateTime =
            if let Value::Timestamp(timestamp) = result.next().unwrap()[0] {
                timestamp
            } else {
                panic!("Wrong type returned!")
            };
        assert_eq!(result, timestamp);
        temp_dir.close()?;
        Ok(())
    }

    #[test]
    fn test_node() -> Result<()> {
        let temp_dir = tempdir::TempDir::new("example")?;
        let db = Database::new(temp_dir.path(), 0)?;
        let conn = Connection::new(&db)?;
        conn.query("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY(name));")?;
        conn.query("CREATE (:Person {name: \"Alice\", age: 25});")?;
        let result = conn.query("MATCH (a:Person) RETURN a;")?.next().unwrap();
        assert_eq!(
            result[0],
            Value::Node(NodeVal {
                id: InternalID {
                    table_id: 0,
                    offset: 0
                },
                label: "Person".to_string(),
                properties: vec![
                    ("name".to_string(), Value::String("Alice".to_string())),
                    ("age".to_string(), Value::Int64(25))
                ]
            })
        );
        temp_dir.close()?;
        Ok(())
    }

    #[test]
    /// Test that null values are read correctly by the API
    fn test_null() -> Result<()> {
        let temp_dir = tempdir::TempDir::new("example")?;
        let db = Database::new(temp_dir.path(), 0)?;
        let conn = Connection::new(&db)?;
        let result = conn.query("RETURN null")?.next();
        let result = &result.unwrap()[0];
        assert_eq!(result, &Value::Null(LogicalType::String));
        temp_dir.close()?;
        Ok(())
    }
}
