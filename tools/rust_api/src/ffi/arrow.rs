#[repr(transparent)]
pub struct ArrowArray(pub arrow::ffi::FFI_ArrowArray);

#[repr(transparent)]
pub struct ArrowSchema(pub arrow::ffi::FFI_ArrowSchema);

unsafe impl cxx::ExternType for ArrowArray {
    type Id = cxx::type_id!("ArrowArray");
    type Kind = cxx::kind::Trivial;
}

unsafe impl cxx::ExternType for ArrowSchema {
    type Id = cxx::type_id!("ArrowSchema");
    type Kind = cxx::kind::Trivial;
}

#[cxx::bridge]
pub(crate) mod ffi_arrow {
    unsafe extern "C++" {
        include!("kuzu/include/kuzu_arrow.h");

        #[namespace = "kuzu::main"]
        type QueryResult = crate::ffi::ffi::QueryResult;
    }

    unsafe extern "C++" {
        type ArrowArray = crate::ffi::arrow::ArrowArray;

        #[namespace = "kuzu_arrow"]
        fn query_result_get_next_arrow_chunk(
            result: Pin<&mut QueryResult>,
            chunk_size: u64,
        ) -> Result<ArrowArray>;
    }

    unsafe extern "C++" {
        type ArrowSchema = crate::ffi::arrow::ArrowSchema;

        #[namespace = "kuzu_arrow"]
        fn query_result_get_arrow_schema(result: &QueryResult) -> Result<ArrowSchema>;
    }
}
