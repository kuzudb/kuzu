use std::env;
use std::path::{Path, PathBuf};

fn link_mode() -> &'static str {
    if env::var("KUZU_SHARED").is_ok() {
        "dylib"
    } else {
        "static"
    }
}

fn find_openssl_windows() {
    // Find openssl library relative to the path of the openssl executable
    // Or fall back to OPENSSL_DIR
    #[cfg(windows)]
    {
        let openssl_dir = if let Ok(mut path) = which::which("openssl") {
            path.pop();
            path.pop();
            path
        } else if let Ok(path) = env::var("OPENSSL_CONF") {
            Path::new(&path)
                .parent()
                .unwrap()
                .parent()
                .unwrap()
                .to_path_buf()
        } else if let Ok(path) = env::var("OPENSSL_DIR") {
            Path::new(&path).to_path_buf()
        } else {
            panic!(
                "OPENSSL_DIR must be set if the openssl library cannot be found \
                            using the path of the openssl executable"
            )
        };
        println!(
            "cargo:rustc-link-search=native={}/lib",
            openssl_dir.display()
        );
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
    println!("cargo:rustc-link-lib={}=kuzu", link_mode());
    if link_mode() == "static" {
        if cfg!(windows) {
            if get_target() == "debug" {
                println!("cargo:rustc-link-lib=dylib=msvcrtd");
            } else {
                println!("cargo:rustc-link-lib=dylib=msvcrt");
            }
            println!("cargo:rustc-link-lib=dylib=shell32");
            println!("cargo:rustc-link-lib=dylib=ole32");
        } else if cfg!(target_os = "macos") {
            println!("cargo:rustc-link-lib=dylib=c++");
        } else {
            println!("cargo:rustc-link-lib=dylib=stdc++");
        }

        println!("cargo:rustc-link-lib=static=arrow_bundled_dependencies");
        if env::var("KUZU_TESTING").is_ok() && cfg!(windows) {
            find_openssl_windows();
            println!("cargo:rustc-link-lib=dylib=libssl");
            println!("cargo:rustc-link-lib=dylib=libcrypto");
        }

        if cfg!(windows) {
            println!("cargo:rustc-link-lib=static=parquet_static");
            println!("cargo:rustc-link-lib=static=arrow_static");
        } else {
            println!("cargo:rustc-link-lib=static=parquet");
            println!("cargo:rustc-link-lib=static=arrow");
        }

        println!("cargo:rustc-link-lib=static=utf8proc");
        println!("cargo:rustc-link-lib=static=antlr4_cypher");
        println!("cargo:rustc-link-lib=static=antlr4_runtime");
        println!("cargo:rustc-link-lib=static=re2");
        println!("cargo:rustc-link-lib=static=serd");
        println!("cargo:rustc-link-lib=static=fastpfor");
        println!("cargo:rustc-link-lib=static=miniparquet");
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
    let mut arrow_build = cmake::Config::new(kuzu_root.join("external"));
    arrow_build
        .no_build_target(true)
        // Needs separate out directory so they don't clobber each other
        .out_dir(Path::new(&env::var("OUT_DIR").unwrap()).join("arrow"));

    if cfg!(windows) {
        arrow_build.generator("Ninja");
        arrow_build.cxxflag("/EHsc");
    }
    let arrow_build_dir = arrow_build.build();

    let arrow_install = arrow_build_dir.join("build/arrow/install");
    println!(
        "cargo:rustc-link-search=native={}",
        arrow_install.join("lib").display()
    );
    println!(
        "cargo:rustc-link-search=native={}",
        arrow_install.join("lib64").display()
    );

    let mut build = cmake::Config::new(&kuzu_root);
    build
        .no_build_target(true)
        .define("BUILD_SHELL", "OFF")
        .define("BUILD_PYTHON_API", "OFF")
        .define("ARROW_INSTALL", &arrow_install);
    if cfg!(windows) {
        build.generator("Ninja");
        build.cxxflag("/EHsc");
    }
    let build_dir = build.build();

    let kuzu_lib_path = build_dir.join("build").join("src");
    println!("cargo:rustc-link-search=native={}", kuzu_lib_path.display());

    for dir in [
        "utf8proc",
        "antlr4_cypher",
        "antlr4_runtime",
        "re2",
        "serd",
        "fastpfor",
        "miniparquet",
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
        kuzu_root.join("third_party/spdlog"),
        kuzu_root.join("third_party/fastpfor"),
        arrow_install.join("include"),
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
    if link_mode() == "static" {
        build.define("KUZU_STATIC_DEFINE", None);
    }

    build.includes(include_paths);

    link_libraries();

    println!("cargo:rerun-if-env-changed=KUZU_SHARED");

    println!("cargo:rerun-if-changed=include/kuzu_rs.h");
    println!("cargo:rerun-if-changed=include/kuzu_rs.cpp");

    if cfg!(windows) {
        build.flag("/std:c++20");
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
        if cfg!(windows) && link_mode() == "dylib" {
            println!("cargo:rustc-link-lib=dylib=kuzu_shared");
        } else {
            println!("cargo:rustc-link-lib={}=kuzu", link_mode());
        }
        include_paths.push(Path::new(&kuzu_include).to_path_buf());
    } else {
        include_paths.extend(build_bundled_cmake().expect("Bundled build failed!"));
        bundled = true;
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
}
