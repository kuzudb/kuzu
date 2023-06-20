use std::env;
use std::path::Path;

fn link_mode() -> &'static str {
    if env::var("KUZU_SHARED").is_ok() {
        "dylib"
    } else {
        "static"
    }
}

fn main() {
    // There is a kuzu-src symlink pointing to the root of the repo since Cargo
    // only looks at the files within the rust project when packaging crates.
    // Using a symlink the library can both be built in-source and from a crate.
    let kuzu_root = {
        let root = Path::new(&std::env::var("CARGO_MANIFEST_DIR").unwrap()).join("kuzu-src");
        if root.is_dir() {
            root
        } else {
            // If the path is not directory, this is probably an in-source build on windows where the
            // symlink is unreadable.
            Path::new(&std::env::var("CARGO_MANIFEST_DIR").unwrap()).join("../..")
        }
    };
    // Windows fails to link on windows unless CFLAGS and CXXFLAGS are overridden
    // If they are, assume the user knows what they are doing. Otherwise just link against the
    // release version of kuzu even in debug mode.
    let target = if cfg!(windows) && std::env::var("CXXFLAGS").is_err() {
        "release".to_string()
    } else {
        env::var("PROFILE").unwrap()
    };
    let kuzu_cmake_root = kuzu_root.join(format!("build/{target}"));
    let mut command = std::process::Command::new("make");
    let threads = env::var("NUM_THREADS")
        .map(|x| x.parse::<usize>())
        .unwrap_or_else(|_| Ok(num_cpus::get()))
        .unwrap();
    command
        .args(&[&target, &format!("NUM_THREADS={}", threads)])
        .current_dir(&kuzu_root);
    let make_status = command.status().unwrap_or_else(|_| {
        panic!(
            "Running make {} on {}/Makefile failed!",
            &target,
            &kuzu_root.display()
        )
    });
    assert!(make_status.success());

    let kuzu_lib_path = kuzu_cmake_root.join("src");

    println!("cargo:rustc-link-search=native={}", kuzu_lib_path.display());

    let include_paths = vec![
        Path::new(&std::env::var("CARGO_MANIFEST_DIR").unwrap()).join("include"),
        kuzu_root.join("src/include"),
        kuzu_root.join("third_party/nlohmann_json"),
        kuzu_root.join("third_party/spdlog"),
    ];
    for dir in ["utf8proc", "antlr4_cypher", "antlr4_runtime", "re2"] {
        let lib_path = kuzu_cmake_root
            .join(format!("third_party/{dir}"))
            .canonicalize()
            .unwrap_or_else(|_| {
                panic!(
                    "Could not find {}/third_party/{dir}",
                    kuzu_cmake_root.display()
                )
            });
        println!("cargo:rustc-link-search=native={}", lib_path.display());
    }

    let arrow_install = kuzu_root.join("external/build/arrow/install");
    println!(
        "cargo:rustc-link-search=native={}",
        arrow_install.join("lib").display()
    );
    println!(
        "cargo:rustc-link-search=native={}",
        arrow_install.join("lib64").display()
    );

    println!("cargo:rustc-link-lib={}=kuzu", link_mode());
    if link_mode() == "static" {
        if cfg!(windows) {
            if target == "debug" {
                println!("cargo:rustc-link-lib=dylib=msvcrtd");
            } else {
                println!("cargo:rustc-link-lib=dylib=msvcrt");
            }
            println!("cargo:rustc-link-lib=dylib=shell32");
            println!("cargo:rustc-link-lib=dylib=ole32");
        } else {
            println!("cargo:rustc-link-lib=dylib=stdc++");
        }

        println!("cargo:rustc-link-lib=static=arrow_bundled_dependencies");
        // arrow's bundled dependencies link against openssl when it's on the system, whether
        // requested or not.
        // Only seems to be necessary when building tests.
        if env::var("KUZU_TESTING").is_ok() {
            if cfg!(windows) {
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
                println!("cargo:rustc-link-lib=dylib=libssl");
                println!("cargo:rustc-link-lib=dylib=libcrypto");
            } else {
                println!("cargo:rustc-link-lib=dylib=ssl");
                println!("cargo:rustc-link-lib=dylib=crypto");
            }
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
    }
    println!("cargo:rerun-if-env-changed=KUZU_SHARED");

    println!("cargo:rerun-if-changed=include/kuzu_rs.h");
    println!("cargo:rerun-if-changed=include/kuzu_rs.cpp");

    let mut build = cxx_build::bridge("src/ffi.rs");
    build.file("src/kuzu_rs.cpp").includes(include_paths);

    if cfg!(windows) {
        build.flag("/std:c++20");
    } else {
        build.flag_if_supported("-std=c++20");
    }
    build.compile("kuzu_rs");
}
