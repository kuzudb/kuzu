use crate::ffi::ffi;

/// Type of [Value](crate::value::Value)s produced and consumed by queries.
///
/// Includes extra type information beyond what can be encoded in [Value](crate::value::Value) such as
/// struct fields and types of lists
#[derive(Clone, Debug, PartialEq)]
pub enum LogicalType {
    /// Special type for use with [Value::Null](crate::value::Value::Null)
    Any,
    /// Correponds to [Value::Bool](crate::value::Value::Bool)
    Bool,
    /// Correponds to [Value::Int64](crate::value::Value::Int64)
    Int64,
    /// Correponds to [Value::Int32](crate::value::Value::Int32)
    Int32,
    /// Correponds to [Value::Int16](crate::value::Value::Int16)
    Int16,
    /// Correponds to [Value::Double](crate::value::Value::Double)
    Double,
    /// Correponds to [Value::Float](crate::value::Value::Float)
    Float,
    /// Correponds to [Value::Date](crate::value::Value::Date)
    Date,
    /// Correponds to [Value::Interval](crate::value::Value::Interval)
    Interval,
    /// Correponds to [Value::Timestamp](crate::value::Value::Timestamp)
    Timestamp,
    /// Correponds to [Value::InternalID](crate::value::Value::InternalID)
    InternalID,
    /// Correponds to [Value::String](crate::value::Value::String)
    String,
    /// Correponds to [Value::VarList](crate::value::Value::VarList)
    VarList { child_type: Box<LogicalType> },
    /// Correponds to [Value::FixedList](crate::value::Value::FixedList)
    FixedList {
        child_type: Box<LogicalType>,
        num_elements: u64,
    },
    /// Correponds to [Value::Struct](crate::value::Value::Struct)
    Struct { fields: Vec<(String, LogicalType)> },
    /// Correponds to [Value::Node](crate::value::Value::Node)
    Node,
    /// Correponds to [Value::Rel](crate::value::Value::Rel)
    Rel,
}

impl From<&ffi::Value> for LogicalType {
    fn from(value: &ffi::Value) -> Self {
        ffi::value_get_data_type(value).as_ref().unwrap().into()
    }
}

impl From<&ffi::LogicalType> for LogicalType {
    fn from(logical_type: &ffi::LogicalType) -> Self {
        use ffi::LogicalTypeID;
        match logical_type.getLogicalTypeID() {
            LogicalTypeID::ANY => LogicalType::Any,
            LogicalTypeID::BOOL => LogicalType::Bool,
            LogicalTypeID::INT16 => LogicalType::Int16,
            LogicalTypeID::INT32 => LogicalType::Int32,
            LogicalTypeID::INT64 => LogicalType::Int64,
            LogicalTypeID::FLOAT => LogicalType::Float,
            LogicalTypeID::DOUBLE => LogicalType::Double,
            LogicalTypeID::STRING => LogicalType::String,
            LogicalTypeID::INTERVAL => LogicalType::Interval,
            LogicalTypeID::DATE => LogicalType::Date,
            LogicalTypeID::TIMESTAMP => LogicalType::Timestamp,
            LogicalTypeID::INTERNAL_ID => LogicalType::InternalID,
            LogicalTypeID::VAR_LIST => LogicalType::VarList {
                child_type: Box::new(
                    ffi::logical_type_get_var_list_child_type(logical_type).into(),
                ),
            },
            LogicalTypeID::FIXED_LIST => LogicalType::FixedList {
                child_type: Box::new(
                    ffi::logical_type_get_fixed_list_child_type(logical_type).into(),
                ),
                num_elements: ffi::logical_type_get_fixed_list_num_elements(logical_type),
            },
            LogicalTypeID::STRUCT => {
                let names = ffi::logical_type_get_struct_field_names(logical_type);
                let types = ffi::logical_type_get_struct_field_types(logical_type);
                LogicalType::Struct {
                    fields: names
                        .into_iter()
                        .zip(types.into_iter().map(Into::<LogicalType>::into))
                        .collect(),
                }
            }
            LogicalTypeID::NODE => LogicalType::Node,
            LogicalTypeID::REL => LogicalType::Rel,
            // Should be unreachable, as cxx will check that the LogicalTypeID enum matches the one
            // on the C++ side.
            x => panic!("Unsupported type {:?}", x),
        }
    }
}

impl From<&LogicalType> for cxx::UniquePtr<ffi::LogicalType> {
    fn from(typ: &LogicalType) -> Self {
        match typ {
            LogicalType::Any
            | LogicalType::Bool
            | LogicalType::Int64
            | LogicalType::Int32
            | LogicalType::Int16
            | LogicalType::Float
            | LogicalType::Double
            | LogicalType::Date
            | LogicalType::Timestamp
            | LogicalType::Interval
            | LogicalType::InternalID
            | LogicalType::String
            | LogicalType::Node
            | LogicalType::Rel => ffi::create_logical_type(typ.id()),
            LogicalType::VarList { child_type } => {
                ffi::create_logical_type_var_list(child_type.as_ref().into())
            }
            LogicalType::FixedList {
                child_type,
                num_elements,
            } => ffi::create_logical_type_fixed_list(child_type.as_ref().into(), *num_elements),
            LogicalType::Struct { fields } => {
                let mut builder = ffi::create_type_list();
                let mut names = vec![];
                for (name, typ) in fields {
                    names.push(name.clone());
                    builder.pin_mut().insert(typ.into());
                }
                ffi::create_logical_type_struct(&names, builder)
            }
        }
    }
}

impl LogicalType {
    pub(crate) fn id(&self) -> ffi::LogicalTypeID {
        use ffi::LogicalTypeID;
        match self {
            LogicalType::Any => LogicalTypeID::ANY,
            LogicalType::Bool => LogicalTypeID::BOOL,
            LogicalType::Int16 => LogicalTypeID::INT16,
            LogicalType::Int32 => LogicalTypeID::INT32,
            LogicalType::Int64 => LogicalTypeID::INT64,
            LogicalType::Float => LogicalTypeID::FLOAT,
            LogicalType::Double => LogicalTypeID::DOUBLE,
            LogicalType::String => LogicalTypeID::STRING,
            LogicalType::Interval => LogicalTypeID::INTERVAL,
            LogicalType::Date => LogicalTypeID::DATE,
            LogicalType::Timestamp => LogicalTypeID::TIMESTAMP,
            LogicalType::InternalID => LogicalTypeID::INTERNAL_ID,
            LogicalType::VarList { .. } => LogicalTypeID::VAR_LIST,
            LogicalType::FixedList { .. } => LogicalTypeID::FIXED_LIST,
            LogicalType::Struct { .. } => LogicalTypeID::STRUCT,
            LogicalType::Node => LogicalTypeID::NODE,
            LogicalType::Rel => LogicalTypeID::REL,
        }
    }
}
