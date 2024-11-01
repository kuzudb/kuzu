use std::env;
use std::path::{Path, PathBuf};

fn link_mode() -> &'static str {
    if env::var("KUZU_SHARED").is_ok() {
        "dylib"
    } else {
        "static"
    }
}

fn get_target() -> String {
    if cfg!(windows) && std::env::var("CXXFLAGS").is_err() {
        "release".to_string()
    } else {
        env::var("PROFILE").unwrap()
    }
}

fn link_libraries() {
    if cfg!(windows) && link_mode() == "dylib" {
        println!("cargo:rustc-link-lib=dylib=kuzu_shared");
    } else {
        println!("cargo:rustc-link-lib={}=kuzu", link_mode());
    }
    if link_mode() == "static" {
        if cfg!(windows) {
            println!("cargo:rustc-link-lib=dylib=msvcrt");
            println!("cargo:rustc-link-lib=dylib=shell32");
            println!("cargo:rustc-link-lib=dylib=ole32");
        } else if cfg!(target_os = "macos") {
            println!("cargo:rustc-link-lib=dylib=c++");
        } else {
            println!("cargo:rustc-link-lib=dylib=stdc++");
        }

        println!("cargo:rustc-link-lib=static=utf8proc");
        println!("cargo:rustc-link-lib=static=antlr4_cypher");
        println!("cargo:rustc-link-lib=static=antlr4_runtime");
        println!("cargo:rustc-link-lib=static=re2");
        println!("cargo:rustc-link-lib=static=fastpfor");
        println!("cargo:rustc-link-lib=static=parquet");
        println!("cargo:rustc-link-lib=static=thrift");
        println!("cargo:rustc-link-lib=static=snappy");
        println!("cargo:rustc-link-lib=static=zstd");
        println!("cargo:rustc-link-lib=static=miniz");
        println!("cargo:rustc-link-lib=static=mbedtls");
        println!("cargo:rustc-link-lib=static=brotlidec");
        println!("cargo:rustc-link-lib=static=brotlicommon");
        println!("cargo:rustc-link-lib=static=lz4");
        println!("cargo:rustc-link-lib=static=roaring_bitmap");
    }
}

fn build_bundled_cmake() -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
    let kuzu_root = {
        let root = Path::new(&std::env::var("CARGO_MANIFEST_DIR").unwrap()).join("kuzu-src");
        if root.is_symlink() || root.is_dir() {
            root
        } else {
            // If the path is not directory, this is probably an in-source build on windows where the
            // symlink is unreadable.
            Path::new(&std::env::var("CARGO_MANIFEST_DIR").unwrap()).join("../..")
        }
    };

    let mut build = cmake::Config::new(&kuzu_root);
    build
        .no_build_target(true)
        .define("BUILD_SHELL", "OFF")
        .define("BUILD_SINGLE_FILE_HEADER", "OFF")
        .define("AUTO_UPDATE_GRAMMAR", "OFF");
    if cfg!(windows) {
        build.generator("Ninja");
        build.cxxflag("/EHsc");
        build.define("CMAKE_MSVC_RUNTIME_LIBRARY", "MultiThreadedDLL");
        build.define("CMAKE_POLICY_DEFAULT_CMP0091", "NEW");
    }
    if let Ok(jobs) = std::env::var("NUM_JOBS") {
        std::env::set_var("CMAKE_BUILD_PARALLEL_LEVEL", jobs);
    }
    let build_dir = build.build();

    let kuzu_lib_path = build_dir.join("build").join("src");
    println!("cargo:rustc-link-search=native={}", kuzu_lib_path.display());

    for dir in [
        "utf8proc",
        "antlr4_cypher",
        "antlr4_runtime",
        "re2",
        "brotli",
        "alp",
        "fastpfor",
        "parquet",
        "thrift",
        "snappy",
        "zstd",
        "miniz",
        "mbedtls",
        "lz4",
        "roaring_bitmap",
    ] {
        let lib_path = build_dir
            .join("build")
            .join("third_party")
            .join(dir)
            .canonicalize()
            .unwrap_or_else(|_| {
                panic!(
                    "Could not find {}/build/third_party/{}",
                    build_dir.display(),
                    dir
                )
            });
        println!("cargo:rustc-link-search=native={}", lib_path.display());
    }

    Ok(vec![
        kuzu_root.join("src/include"),
        build_dir.join("build/src"),
        kuzu_root.join("third_party/nlohmann_json"),
        kuzu_root.join("third_party/fastpfor"),
        kuzu_root.join("third_party/alp/include"),
    ])
}

fn build_ffi(
    bridge_file: &str,
    out_name: &str,
    source_file: &str,
    bundled: bool,
    include_paths: &Vec<PathBuf>,
) {
    let mut build = cxx_build::bridge(bridge_file);
    build.file(source_file);

    if bundled {
        build.define("KUZU_BUNDLED", None);
    }
    if get_target() == "debug" || get_target() == "relwithdebinfo" {
        build.define("ENABLE_RUNTIME_CHECKS", "1");
    }
    if link_mode() == "static" {
        build.define("KUZU_STATIC_DEFINE", None);
    }

    build.includes(include_paths);

    println!("cargo:rerun-if-env-changed=KUZU_SHARED");

    println!("cargo:rerun-if-changed=include/kuzu_rs.h");
    println!("cargo:rerun-if-changed=src/kuzu_rs.cpp");
    // Note that this should match the kuzu-src/* entries in the package.include list in Cargo.toml
    // Unfortunately they appear to need to be specified individually since the symlink is
    // considered to be changed each time.
    println!("cargo:rerun-if-changed=kuzu-src/src");
    println!("cargo:rerun-if-changed=kuzu-src/third_party");
    println!("cargo:rerun-if-changed=kuzu-src/CMakeLists.txt");
    println!("cargo:rerun-if-changed=kuzu-src/tools/CMakeLists.txt");

    if cfg!(windows) {
        build.flag("/std:c++20");
        build.flag("/MD");
    } else {
        build.flag("-std=c++2a");
    }
    build.compile(out_name);
}

fn main() {
    if env::var("DOCS_RS").is_ok() {
        // Do nothing; we're just building docs and don't need the C++ library
        return;
    }

    let mut bundled = false;
    let mut include_paths =
        vec![Path::new(&std::env::var("CARGO_MANIFEST_DIR").unwrap()).join("include")];

    if let (Ok(kuzu_lib_dir), Ok(kuzu_include)) =
        (env::var("KUZU_LIBRARY_DIR"), env::var("KUZU_INCLUDE_DIR"))
    {
        println!("cargo:rustc-link-search=native={}", kuzu_lib_dir);
        // Also add to environment used by cargo run and tests
        println!("cargo:rustc-env=LD_LIBRARY_PATH={}", kuzu_lib_dir);
        include_paths.push(Path::new(&kuzu_include).to_path_buf());
    } else {
        include_paths.extend(build_bundled_cmake().expect("Bundled build failed!"));
        bundled = true;
    }
    if link_mode() == "static" {
        link_libraries();
    }
    build_ffi(
        "src/ffi.rs",
        "kuzu_rs",
        "src/kuzu_rs.cpp",
        bundled,
        &include_paths,
    );

    if cfg!(feature = "arrow") {
        build_ffi(
            "src/ffi/arrow.rs",
            "kuzu_arrow_rs",
            "src/kuzu_arrow.cpp",
            bundled,
            &include_paths,
        );
    }
    if link_mode() == "dylib" {
        link_libraries();
    }
}
