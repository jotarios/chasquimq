use std::fs;
use std::path::PathBuf;

fn main() {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace parent")
        .join("chasquimq")
        .join("Cargo.toml");
    println!("cargo:rerun-if-changed={}", manifest.display());

    let toml = fs::read_to_string(&manifest)
        .unwrap_or_else(|e| panic!("read engine manifest {}: {e}", manifest.display()));
    let version = parse_package_version(&toml)
        .unwrap_or_else(|| panic!("could not find [package] version in {}", manifest.display()));
    println!("cargo:rustc-env=CHASQUIMQ_ENGINE_VERSION={version}");

    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    if target_os == "macos" {
        println!("cargo:rustc-cdylib-link-arg=-undefined");
        println!("cargo:rustc-cdylib-link-arg=dynamic_lookup");
    }
}

fn parse_package_version(toml: &str) -> Option<String> {
    let mut in_package = false;
    for line in toml.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('[') {
            in_package = trimmed == "[package]";
            continue;
        }
        if !in_package {
            continue;
        }
        if let Some(rest) = trimmed.strip_prefix("version") {
            let rest = rest.trim_start();
            let rest = rest.strip_prefix('=')?.trim();
            let rest = rest.trim_matches('"');
            return Some(rest.to_string());
        }
    }
    None
}
