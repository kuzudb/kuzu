use std::env;
use std::path::Path;

fn link_mode() -> &'static str {
    if env::var("KUZU_SHARED").is_ok() {
        "dylib"
    } else {
        "static"
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // There is a kuzu-src symlink pointing to the root of the repo since Cargo
    // only looks at the files within the rust project when packaging crates.
    // Using a symlink the library can both be built in-source and from a crate.
    let kuzu_root = Path::new(&std::env::var("CARGO_MANIFEST_DIR").unwrap()).join("kuzu-src");
    let target = env::var("PROFILE")?;
    let kuzu_cmake_root = kuzu_root.join(format!("build/{target}"));
    let mut command = std::process::Command::new("make");
    command
        .args(&[target, format!("NUM_THREADS={}", num_cpus::get())])
        .current_dir(&kuzu_root);
    let make_status = command.status()?;
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
        println!("cargo:rustc-link-lib=dylib=stdc++");

        println!("cargo:rustc-link-lib=static=arrow_bundled_dependencies");
        // Dependencies of arrow's bundled dependencies
        // Only seems to be necessary when building tests.
        // This will probably not work on windows/macOS
        // openssl-sys has better cross-platform logic, but just using that doesn't work.
        if env::var("KUZU_TESTING").is_ok() {
            println!("cargo:rustc-link-lib=dylib=ssl");
            println!("cargo:rustc-link-lib=dylib=crypto");
        }

        println!("cargo:rustc-link-lib=static=parquet");
        println!("cargo:rustc-link-lib=static=arrow");

        println!("cargo:rustc-link-lib=static=utf8proc");
        println!("cargo:rustc-link-lib=static=antlr4_cypher");
        println!("cargo:rustc-link-lib=static=antlr4_runtime");
        println!("cargo:rustc-link-lib=static=re2");
    }
    println!("cargo:rerun-if-env-changed=KUZU_SHARED");

    println!("cargo:rerun-if-changed=include/kuzu_rs.h");
    println!("cargo:rerun-if-changed=include/kuzu_rs.cpp");

    cxx_build::bridge("src/ffi.rs")
        .file("src/kuzu_rs.cpp")
        .flag_if_supported("-std=c++20")
        .includes(include_paths)
        .compile("kuzu_rs");

    Ok(())
}
