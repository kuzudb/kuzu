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
    pub fn new<I: Into<InternalID>, S: Into<String>>(id: I, label: S) -> Self {
        NodeVal {
            id: id.into(),
            label: label.into(),
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
    pub fn add_property<S: Into<String>, V: Into<Value>>(&mut self, key: S, value: V) {
        self.properties.push((key.into(), value.into()));
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
    pub fn new<I: Into<InternalID>, S: Into<String>>(src_node: I, dst_node: I, label: S) -> Self {
        RelVal {
            src_node: src_node.into(),
            dst_node: dst_node.into(),
            label: label.into(),
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

impl From<(u64, u64)> for InternalID {
    fn from(value: (u64, u64)) -> Self {
        InternalID {
            offset: value.0,
            table_id: value.1,
        }
    }
}

/// Data types supported by Kùzu
///
/// Also see <https://kuzudb.com/docusaurus/cypher/data-types/overview.html>
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null(LogicalType),
    Bool(bool),
    Int64(i64),
    Int32(i32),
    Int16(i16),
    Int8(i8),
    UInt64(u64),
    UInt32(u32),
    UInt16(u16),
    UInt8(u8),
    Int128(i128),
    Double(f64),
    Float(f32),
    /// Stored internally as the number of days since 1970-01-01 as a 32-bit signed integer, which
    /// allows for a wider range of dates to be stored than can be represented by time::Date
    ///
    /// <https://kuzudb.com/docusaurus/cypher/data-types/date.html>
    Date(time::Date),
    /// May be signed or unsigned.
    ///
    /// Nanosecond precision of time::Duration (if available) will not be preserved when passed to
    /// queries, and results will always have at most microsecond precision.
    ///
    /// <https://kuzudb.com/docusaurus/cypher/data-types/interval.html>
    Interval(time::Duration),
    /// Stored internally as the number of microseconds since 1970-01-01
    /// Nanosecond precision of SystemTime (if available) will not be preserved when used.
    ///
    /// <https://kuzudb.com/docusaurus/cypher/data-types/timestamp.html>
    Timestamp(time::OffsetDateTime),
    InternalID(InternalID),
    /// <https://kuzudb.com/docusaurus/cypher/data-types/string.html>
    String(String),
    Blob(Vec<u8>),
    // TODO: Enforce type of contents
    // LogicalType is necessary so that we can pass the correct type to the C++ API if the list is empty.
    /// These must contain elements which are all the given type.
    /// <https://kuzudb.com/docusaurus/cypher/data-types/list.html>
    VarList(LogicalType, Vec<Value>),
    /// These must contain elements which are all the same type.
    /// <https://kuzudb.com/docusaurus/cypher/data-types/list.html>
    FixedList(LogicalType, Vec<Value>),
    /// <https://kuzudb.com/docusaurus/cypher/data-types/struct.html>
    Struct(Vec<(String, Value)>),
    Node(NodeVal),
    Rel(RelVal),
    RecursiveRel {
        /// Interior nodes in the Sequence of Rels
        ///
        /// Does not include the starting or ending Node.
        nodes: Vec<NodeVal>,
        /// Sequence of Rels which make up the RecursiveRel
        rels: Vec<RelVal>,
    },
    /// <https://kuzudb.com/docusaurus/cypher/data-types/map>
    Map((LogicalType, LogicalType), Vec<(Value, Value)>),
    /// <https://kuzudb.com/docusaurus/cypher/data-types/union>
    Union {
        types: Vec<(String, LogicalType)>,
        value: Box<Value>,
    },
}

fn display_list<T: std::fmt::Display>(f: &mut fmt::Formatter<'_>, list: &Vec<T>) -> fmt::Result {
    write!(f, "[")?;
    for (i, value) in list.iter().enumerate() {
        write!(f, "{}", value)?;
        if i != list.len() - 1 {
            write!(f, ",")?;
        }
    }
    write!(f, "]")
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Bool(true) => write!(f, "True"),
            Value::Bool(false) => write!(f, "False"),
            Value::Int8(x) => write!(f, "{x}"),
            Value::Int16(x) => write!(f, "{x}"),
            Value::Int32(x) => write!(f, "{x}"),
            Value::Int64(x) => write!(f, "{x}"),
            Value::UInt8(x) => write!(f, "{x}"),
            Value::UInt16(x) => write!(f, "{x}"),
            Value::UInt32(x) => write!(f, "{x}"),
            Value::UInt64(x) => write!(f, "{x}"),
            Value::Int128(x) => write!(f, "{x}"),
            Value::Date(x) => write!(f, "{x}"),
            Value::String(x) => write!(f, "{x}"),
            Value::Blob(x) => write!(f, "{x:x?}"),
            Value::Null(_) => write!(f, ""),
            Value::VarList(_, x) | Value::FixedList(_, x) => display_list(f, x),
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
            Value::Map(_, x) => {
                write!(f, "{{")?;
                for (i, (name, value)) in x.iter().enumerate() {
                    write!(f, "{}={}", name, value)?;
                    if i != x.len() - 1 {
                        write!(f, ", ")?;
                    }
                }
                write!(f, "}}")
            }
            Value::Node(x) => write!(f, "{x}"),
            Value::Rel(x) => write!(f, "{x}"),
            Value::InternalID(x) => write!(f, "{x}"),
            Value::RecursiveRel { nodes, rels } => {
                write!(f, "{{")?;
                write!(f, "_NODES: ")?;
                display_list(f, nodes)?;
                write!(f, ", _RELS: ")?;
                display_list(f, rels)?;
                write!(f, "}}")
            }
            Value::Union { types: _, value } => write!(f, "{value}"),
        }
    }
}

impl From<&Value> for LogicalType {
    fn from(value: &Value) -> Self {
        match value {
            Value::Bool(_) => LogicalType::Bool,
            Value::Int8(_) => LogicalType::Int8,
            Value::Int16(_) => LogicalType::Int16,
            Value::Int32(_) => LogicalType::Int32,
            Value::Int64(_) => LogicalType::Int64,
            Value::UInt8(_) => LogicalType::UInt8,
            Value::UInt16(_) => LogicalType::UInt16,
            Value::UInt32(_) => LogicalType::UInt32,
            Value::UInt64(_) => LogicalType::UInt64,
            Value::Int128(_) => LogicalType::Int128,
            Value::Float(_) => LogicalType::Float,
            Value::Double(_) => LogicalType::Double,
            Value::Date(_) => LogicalType::Date,
            Value::Interval(_) => LogicalType::Interval,
            Value::Timestamp(_) => LogicalType::Timestamp,
            Value::String(_) => LogicalType::String,
            Value::Blob(_) => LogicalType::Blob,
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
            Value::RecursiveRel { .. } => LogicalType::RecursiveRel,
            Value::Map((key_type, value_type), _) => LogicalType::Map {
                key_type: Box::new(key_type.clone()),
                value_type: Box::new(value_type.clone()),
            },
            Value::Union { types, value: _ } => LogicalType::Union {
                types: types.clone(),
            },
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
            LogicalTypeID::INT8 => Ok(Value::Int8(value.get_value_i8())),
            LogicalTypeID::INT16 => Ok(Value::Int16(value.get_value_i16())),
            LogicalTypeID::INT32 => Ok(Value::Int32(value.get_value_i32())),
            LogicalTypeID::INT64 => Ok(Value::Int64(value.get_value_i64())),
            LogicalTypeID::UINT8 => Ok(Value::UInt8(value.get_value_u8())),
            LogicalTypeID::UINT16 => Ok(Value::UInt16(value.get_value_u16())),
            LogicalTypeID::UINT32 => Ok(Value::UInt32(value.get_value_u32())),
            LogicalTypeID::UINT64 => Ok(Value::UInt64(value.get_value_u64())),
            LogicalTypeID::INT128 => {
                let int128_val = ffi::value_get_int128_t(value);
                let low = int128_val[1];
                let high = int128_val[0] as i64;
                Ok(Value::Int128(
                    (low as i128) + ((high as i128) * (u64::MAX as i128)),
                ))
            }
            LogicalTypeID::FLOAT => Ok(Value::Float(value.get_value_float())),
            LogicalTypeID::DOUBLE => Ok(Value::Double(value.get_value_double())),
            LogicalTypeID::STRING => Ok(Value::String(ffi::value_get_string(value).to_string())),
            LogicalTypeID::BLOB => Ok(Value::Blob(
                ffi::value_get_string(value).as_bytes().to_vec(),
            )),
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
                let mut result = vec![];
                for index in 0..ffi::value_get_children_size(value) {
                    let value: Value = ffi::value_get_child(value, index).try_into()?;
                    result.push(value);
                }
                if let LogicalType::VarList { child_type } = value.into() {
                    Ok(Value::VarList(*child_type, result))
                } else {
                    unreachable!()
                }
            }
            LogicalTypeID::FIXED_LIST => {
                let mut result = vec![];
                for index in 0..ffi::value_get_children_size(value) {
                    let value: Value = ffi::value_get_child(value, index).try_into()?;
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
                let field_names =
                    ffi::logical_type_get_struct_field_names(ffi::value_get_data_type(value));
                let mut result = vec![];
                for (name, index) in field_names
                    .into_iter()
                    .zip(0..ffi::value_get_children_size(value))
                {
                    let value: Value = ffi::value_get_child(value, index).try_into()?;
                    result.push((name, value));
                }
                Ok(Value::Struct(result))
            }
            LogicalTypeID::MAP => {
                let mut result = vec![];
                for index in 0..ffi::value_get_children_size(value) {
                    let pair = ffi::value_get_child(value, index);
                    result.push((
                        ffi::value_get_child(pair, 0).try_into()?,
                        ffi::value_get_child(pair, 1).try_into()?,
                    ));
                }
                if let LogicalType::Map {
                    key_type,
                    value_type,
                } = value.into()
                {
                    Ok(Value::Map((*key_type, *value_type), result))
                } else {
                    unreachable!()
                }
            }
            LogicalTypeID::NODE => {
                let id = ffi::node_value_get_node_id(value);
                let id = InternalID {
                    offset: id[0],
                    table_id: id[1],
                };
                let label = ffi::node_value_get_label_name(value);
                let mut node_val = NodeVal::new(id, label);
                for i in 0..ffi::node_value_get_num_properties(value) {
                    node_val.add_property(
                        ffi::node_value_get_property_name(value, i),
                        TryInto::<Value>::try_into(ffi::node_value_get_property_value(value, i))?,
                    );
                }
                Ok(Value::Node(node_val))
            }
            LogicalTypeID::REL => {
                let src_node = ffi::rel_value_get_src_id(value);
                let dst_node = ffi::rel_value_get_dst_id(value);
                let src_node = InternalID {
                    offset: src_node[0],
                    table_id: src_node[1],
                };
                let dst_node = InternalID {
                    offset: dst_node[0],
                    table_id: dst_node[1],
                };
                let label = ffi::rel_value_get_label_name(value);
                let mut rel_val = RelVal::new(src_node, dst_node, label);
                for i in 0..ffi::rel_value_get_num_properties(value) {
                    rel_val.add_property(
                        ffi::rel_value_get_property_name(value, i),
                        ffi::rel_value_get_property_value(value, i).try_into()?,
                    );
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
            LogicalTypeID::RECURSIVE_REL => {
                let nodes: Value = ffi::recursive_rel_get_nodes(value).try_into()?;
                let rels: Value = ffi::recursive_rel_get_rels(value).try_into()?;
                let nodes = if let Value::VarList(LogicalType::Node, nodes) = nodes {
                    nodes.into_iter().map(|x| {
                        if let Value::Node(x) = x {
                            x
                        } else {
                            unreachable!()
                        }
                    })
                } else {
                    panic!("Unexpected value in RecursiveRel's rels: {}", rels)
                };
                let rels = if let Value::VarList(LogicalType::Rel, rels) = rels {
                    rels.into_iter().map(|x| {
                        if let Value::Rel(x) = x {
                            x
                        } else {
                            unreachable!()
                        }
                    })
                } else {
                    panic!("Unexpected value in RecursiveRel's rels: {}", rels)
                };

                Ok(Value::RecursiveRel {
                    nodes: nodes.collect(),
                    rels: rels.collect(),
                })
            }
            LogicalTypeID::UNION => {
                let types =
                    if let LogicalType::Union { types } = ffi::value_get_data_type(value).into() {
                        types
                    } else {
                        unreachable!()
                    };
                let value: Value = ffi::value_get_child(value, 0).try_into()?;
                Ok(Value::Union {
                    types,
                    value: Box::new(value),
                })
            }
            // TODO(bmwinger): Better error message for types which are unsupported
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
            Value::Int8(value) => Ok(ffi::create_value_i8(value)),
            Value::Int16(value) => Ok(ffi::create_value_i16(value)),
            Value::Int32(value) => Ok(ffi::create_value_i32(value)),
            Value::Int64(value) => Ok(ffi::create_value_i64(value)),
            Value::UInt8(value) => Ok(ffi::create_value_u8(value)),
            Value::UInt16(value) => Ok(ffi::create_value_u16(value)),
            Value::UInt32(value) => Ok(ffi::create_value_u32(value)),
            Value::UInt64(value) => Ok(ffi::create_value_u64(value)),
            Value::Int128(value) => {
                let low = (value % (u64::MAX as i128)) as u64;
                let high = ((value - low as i128) / (u64::MAX as i128)) as i64;
                Ok(ffi::create_value_int128_t(high, low))
            }
            Value::Float(value) => Ok(ffi::create_value_float(value)),
            Value::Double(value) => Ok(ffi::create_value_double(value)),
            Value::String(value) => Ok(ffi::create_value_string(
                ffi::LogicalTypeID::STRING,
                value.as_bytes(),
            )),
            Value::Blob(value) => Ok(ffi::create_value_string(ffi::LogicalTypeID::BLOB, &value)),
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
            Value::Map((key_type, value_type), values) => {
                let mut builder = ffi::create_list();
                let list_type = LogicalType::Struct {
                    fields: vec![
                        ("KEY".to_string(), key_type.clone()),
                        ("VALUE".to_string(), value_type.clone()),
                    ],
                };
                for (key, value) in values {
                    let mut pair = ffi::create_list();
                    pair.pin_mut().insert(key.try_into()?);
                    pair.pin_mut().insert(value.try_into()?);
                    let pair_value = ffi::get_list_value((&list_type).into(), pair);
                    builder.pin_mut().insert(pair_value);
                }
                Ok(ffi::get_list_value(
                    (&LogicalType::Map {
                        key_type: Box::new(key_type),
                        value_type: Box::new(value_type),
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
            Value::Node(_) => Err(crate::Error::ReadOnlyType(LogicalType::Node)),
            Value::Rel(_) => Err(crate::Error::ReadOnlyType(LogicalType::Rel)),
            Value::RecursiveRel { .. } => {
                Err(crate::Error::ReadOnlyType(LogicalType::RecursiveRel))
            }
            Value::Union { types, value } => {
                let mut builder = ffi::create_list();
                builder.pin_mut().insert((*value).try_into()?);

                Ok(ffi::get_list_value(
                    (&LogicalType::Union { types }).into(),
                    builder,
                ))
            }
        }
    }
}

impl From<i8> for Value {
    fn from(item: i8) -> Self {
        Value::Int8(item)
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

impl From<u8> for Value {
    fn from(item: u8) -> Self {
        Value::UInt8(item)
    }
}

impl From<u16> for Value {
    fn from(item: u16) -> Self {
        Value::UInt16(item)
    }
}

impl From<u32> for Value {
    fn from(item: u32) -> Self {
        Value::UInt32(item)
    }
}

impl From<u64> for Value {
    fn from(item: u64) -> Self {
        Value::UInt64(item)
    }
}

impl From<i128> for Value {
    fn from(item: i128) -> Self {
        Value::Int128(item)
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
        Connection, Database, InternalID, LogicalType, NodeVal, RelVal, SystemConfig, Value,
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
            /// Tests that the values display the same via the rust API as via the C++ API
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
                let temp_dir = tempfile::tempdir()?;
                let db = Database::new(temp_dir.path(), SystemConfig::default())?;
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
        convert_int8_type: LogicalType::Int8,
        convert_int16_type: LogicalType::Int16,
        convert_int32_type: LogicalType::Int32,
        convert_int64_type: LogicalType::Int64,
        convert_uint8_type: LogicalType::UInt8,
        convert_uint16_type: LogicalType::UInt16,
        convert_uint32_type: LogicalType::UInt32,
        convert_uint64_type: LogicalType::UInt64,
        convert_int128_type: LogicalType::Int128,
        convert_float_type: LogicalType::Float,
        convert_double_type: LogicalType::Double,
        convert_timestamp_type: LogicalType::Timestamp,
        convert_date_type: LogicalType::Date,
        convert_interval_type: LogicalType::Interval,
        convert_string_type: LogicalType::String,
        convert_blob_type: LogicalType::Blob,
        convert_bool_type: LogicalType::Bool,
        convert_struct_type: LogicalType::Struct { fields: vec![("NAME".to_string(), LogicalType::String)]},
        convert_node_type: LogicalType::Node,
        convert_internal_id_type: LogicalType::InternalID,
        convert_rel_type: LogicalType::Rel,
        convert_recursive_rel_type: LogicalType::RecursiveRel,
        convert_map_type: LogicalType::Map { key_type: Box::new(LogicalType::Interval), value_type: Box::new(LogicalType::Rel) },
        convert_union_type: LogicalType::Union { types: vec![("Num".to_string(), LogicalType::Int8), ("duration".to_string(), LogicalType::Interval), ("string".to_string(), LogicalType::String)] },
    }

    value_tests! {
        convert_var_list: Value::VarList(LogicalType::String, vec!["Alice".into(), "Bob".into()]),
        convert_var_list_empty: Value::VarList(LogicalType::String, vec![]),
        convert_fixed_list: Value::FixedList(LogicalType::String, vec!["Alice".into(), "Bob".into()]),
        convert_int8: Value::Int8(0),
        convert_int16: Value::Int16(1),
        convert_int32: Value::Int32(2),
        convert_int64: Value::Int64(3),
        convert_uint8: Value::UInt8(0),
        convert_uint16: Value::UInt16(1),
        convert_uint32: Value::UInt32(2),
        convert_uint64: Value::UInt64(3),
        convert_int128: Value::Int128(1),
        convert_int128_large: Value::Int128(184467440737095516158),
        convert_float: Value::Float(4.),
        convert_double: Value::Double(5.),
        convert_timestamp: Value::Timestamp(datetime!(2023-06-13 11:25:30 UTC)),
        convert_date: Value::Date(date!(2023-06-13)),
        convert_interval: Value::Interval(time::Duration::weeks(10)),
        convert_string: Value::String("Hello World".to_string()),
        convert_blob: Value::Blob("Hello World".into()),
        convert_bool: Value::Bool(false),
        convert_null: Value::Null(LogicalType::VarList {
            child_type: Box::new(LogicalType::FixedList { child_type: Box::new(LogicalType::Int16), num_elements: 3 })
        }),
        convert_struct: Value::Struct(vec![("NAME".to_string(), "Alice".into()), ("AGE".to_string(), 25.into())]),
        convert_internal_id: Value::InternalID(InternalID { table_id: 0, offset: 0 }),
        convert_map: Value::Map((LogicalType::String, LogicalType::Int64), vec![(Value::String("key".to_string()), Value::Int64(24))]),
        convert_union: Value::Union {
            types: vec![("Num".to_string(), LogicalType::Int8), ("duration".to_string(), LogicalType::Interval)],
            value: Box::new(Value::Int8(-127))
        },
    }

    display_tests! {
        display_var_list: Value::VarList(LogicalType::String, vec!["Alice".into(), "Bob".into()]),
        display_var_list_empty: Value::VarList(LogicalType::String, vec![]),
        display_fixed_list: Value::FixedList(LogicalType::String, vec!["Alice".into(), "Bob".into()]),
        display_int8: Value::Int8(0),
        display_int16: Value::Int16(1),
        display_int32: Value::Int32(2),
        display_int64: Value::Int64(3),
        display_uint8: Value::UInt8(0),
        display_uint16: Value::UInt16(1),
        display_uint32: Value::UInt32(2),
        display_uint64: Value::UInt64(3),
        // Float, double, interval and timestamp have display differences which we probably don't want to
        // reconcile
        display_date: Value::Date(date!(2023-06-13)),
        // blob may contain non-utf8 data, so we display it as an array rather than a string
        // The C++ API escapes data in the blob as hex
        display_string: Value::String("Hello World".to_string()),
        display_bool: Value::Bool(false),
        display_null: Value::Null(LogicalType::VarList {
            child_type: Box::new(LogicalType::FixedList { child_type: Box::new(LogicalType::Int16), num_elements: 3 })
        }),
        display_struct: Value::Struct(vec![("NAME".to_string(), "Alice".into()), ("AGE".to_string(), 25.into())]),
        display_internal_id: Value::InternalID(InternalID { table_id: 0, offset: 0 }),
        // Node and Rel Cannot be easily created on the C++ side
        display_map: Value::Map((LogicalType::String, LogicalType::Int64), vec![(Value::String("key".to_string()), Value::Int64(24))]),
        display_union: Value::Union {
            types: vec![("Num".to_string(), LogicalType::Int8), ("duration".to_string(), LogicalType::Interval)],
            value: Box::new(Value::Int8(-127))
        },
    }

    database_tests! {
        // Passing these values as arguments is not yet implemented in kuzu:
        // db_var_list_string: Value::VarList(LogicalType::String, vec!["Alice".into(), "Bob".into()]), "STRING[]",
        // db_var_list_int: Value::VarList(LogicalType::Int64, vec![0i64.into(), 1i64.into(), 2i64.into()]), "INT64[]",
        // db_map: Value::Map((LogicalType::String, LogicalType::Int64), vec![(Value::String("key".to_string()), Value::Int64(24))]), "MAP(STRING,INT64)",
        // db_fixed_list: Value::FixedList(LogicalType::Int64, vec![1i64.into(), 2i64.into(), 3i64.into()]), "INT64[3]",
        // db_union: Value::Union {
        //    types: vec![("Num".to_string(), LogicalType::Int8), ("duration".to_string(), LogicalType::Interval)],
        //    value: Box::new(Value::Int8(-127))
        // }, "UNION(Num INT8, duration INTERVAL)",
        db_struct:
           Value::Struct(vec![("item".to_string(), "Knife".into()), ("count".to_string(), 1.into())]),
           "STRUCT(item STRING, count INT32)",
        db_null_string: Value::Null(LogicalType::String), "STRING",
        db_null_int: Value::Null(LogicalType::Int64), "INT64",
        db_null_list: Value::Null(LogicalType::VarList {
           child_type: Box::new(LogicalType::FixedList { child_type: Box::new(LogicalType::Int16), num_elements: 3 })
        }), "INT16[3][]",
        db_int8: Value::Int8(0), "INT8",
        db_int16: Value::Int16(1), "INT16",
        db_int32: Value::Int32(2), "INT32",
        db_int64: Value::Int64(3), "INT64",
        db_uint8: Value::UInt8(0), "UINT8",
        db_uint16: Value::UInt16(1), "UINT16",
        db_uint32: Value::UInt32(2), "UINT32",
        db_uint64: Value::UInt64(3), "UINT64",
        db_int128: Value::Int128(4), "INT128",
        db_float: Value::Float(4.), "FLOAT",
        db_double: Value::Double(5.), "DOUBLE",
        db_timestamp: Value::Timestamp(datetime!(2023-06-13 11:25:30 UTC)), "TIMESTAMP",
        db_date: Value::Date(date!(2023-06-13)), "DATE",
        db_interval: Value::Interval(time::Duration::weeks(200)), "INTERVAL",
        db_string: Value::String("Hello World".to_string()), "STRING",
        db_blob: Value::Blob("Hello World".into()), "BLOB",
        db_bool: Value::Bool(true), "BOOLEAN",
    }

    #[test]
    /// Tests that the list value is correctly constructed
    fn test_var_list_get() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let db = Database::new(temp_dir.path(), SystemConfig::default())?;
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
        let temp_dir = tempfile::tempdir()?;
        let db = Database::new(temp_dir.path(), SystemConfig::default())?;
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
        let temp_dir = tempfile::tempdir()?;
        let db = Database::new(temp_dir.path(), SystemConfig::default())?;
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
    fn test_recursive_rel() -> Result<()> {
        let temp_dir = tempfile::TempDir::new()?;
        let db = Database::new(temp_dir.path(), SystemConfig::default())?;
        let conn = Connection::new(&db)?;
        conn.query("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY(name));")?;
        conn.query("CREATE REL TABLE knows(FROM Person TO Person);")?;
        conn.query("CREATE (:Person {name: \"Alice\", age: 25});")?;
        conn.query("CREATE (:Person {name: \"Bob\", age: 25});")?;
        conn.query("CREATE (:Person {name: \"Eve\", age: 25});")?;
        conn.query(
            "MATCH (p1:Person), (p2:Person)
                WHERE p1.name = \"Alice\" AND p2.name = \"Bob\"
            CREATE (p1)-[:knows]->(p2);",
        )?;
        conn.query(
            "MATCH (p1:Person), (p2:Person)
                WHERE p1.name = \"Bob\" AND p2.name = \"Eve\"
            CREATE (p1)-[:knows]->(p2);",
        )?;
        let result = conn
            .query(
                "MATCH (a:Person)-[e*2..2]->(b:Person)
                 WHERE a.name = 'Alice'
                 RETURN e, b.name;",
            )?
            .next()
            .unwrap();
        assert_eq!(result[1], Value::String("Eve".to_string()));
        assert_eq!(
            result[0],
            Value::RecursiveRel {
                nodes: vec![NodeVal {
                    id: (1, 0).into(),
                    label: "Person".into(),
                    properties: vec![("name".into(), "Bob".into()), ("age".into(), 25i64.into())]
                },],
                rels: vec![
                    RelVal::new((0, 0), (1, 0), "knows"),
                    RelVal::new((1, 0), (2, 0), "knows"),
                ],
            }
        );
        temp_dir.close()?;
        Ok(())
    }

    #[test]
    /// Test that null values are read correctly by the API
    fn test_null() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let db = Database::new(temp_dir.path(), SystemConfig::default())?;
        let conn = Connection::new(&db)?;
        let result = conn.query("RETURN null")?.next();
        let result = &result.unwrap()[0];
        assert_eq!(result, &Value::Null(LogicalType::String));
        temp_dir.close()?;
        Ok(())
    }

    #[test]
    /// Tests that passing the values through the database returns what we put in
    fn test_serial() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let db = Database::new(temp_dir.path(), SystemConfig::default())?;
        let conn = Connection::new(&db)?;
        conn.query("CREATE NODE TABLE Person(id SERIAL, name STRING, PRIMARY KEY(id));")?;
        conn.query("CREATE (:Person {name: \"Bob\"});")?;
        conn.query("CREATE (:Person {name: \"Alice\"});")?;
        let result = conn.query("MATCH (a:Person) RETURN a.name, a.id;")?;
        assert_eq!(
            result.get_column_data_types(),
            vec![LogicalType::String, LogicalType::Serial]
        );
        let results: Vec<(Value, Value)> = result
            .map(|mut x| (x.pop().unwrap(), x.pop().unwrap()))
            .collect();
        assert_eq!(
            results,
            vec![
                (Value::Int64(0), "Bob".into()),
                (Value::Int64(1), "Alice".into())
            ]
        );
        temp_dir.close()?;
        Ok(())
    }

    #[test]
    /// Tests that passing the values through the database returns what we put in
    fn test_union() -> Result<()> {
        use std::fs::File;
        use std::io::Write;
        let temp_dir = tempfile::tempdir()?;
        let db = Database::new(temp_dir.path(), SystemConfig::default())?;
        let conn = Connection::new(&db)?;
        conn.query(
            "CREATE NODE TABLE demo(a SERIAL, b UNION(num INT64, str STRING), PRIMARY KEY(a));",
        )?;
        let mut file = File::create(temp_dir.path().join("demo.csv"))?;
        file.write_all(b"1\naa\n")?;
        conn.query(&format!(
            "COPY demo from '{}/demo.csv';",
            // Use forward-slashes instead of backslashes on windows, as thmay not be supported by
            // the query parser
            temp_dir.path().display().to_string().replace("\\", "/")
        ))?;
        let result = conn.query("MATCH (d:demo) RETURN d.b;")?;
        let types = vec![
            ("num".to_string(), LogicalType::Int64),
            ("str".to_string(), LogicalType::String),
        ];
        assert_eq!(
            result.get_column_data_types(),
            vec![LogicalType::Union {
                types: types.clone()
            }],
        );
        let results: Vec<Value> = result.map(|mut x| x.pop().unwrap()).collect();
        assert_eq!(
            results,
            vec![
                Value::Union {
                    types: types.clone(),
                    value: Box::new(Value::Int64(1))
                },
                Value::Union {
                    types: types.clone(),
                    value: Box::new(Value::String("aa".to_string()))
                },
            ]
        );
        temp_dir.close()?;
        Ok(())
    }
}
