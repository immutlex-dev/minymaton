use std::env;
use std::ffi::OsString;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::process::{self, Command};

fn program(name: &str) -> String {
    format!("{name}{}", env::consts::EXE_SUFFIX)
}

fn run() -> Result<i32, Box<dyn std::error::Error>> {
    let exe = env::current_exe()?.canonicalize()?;
    let bin = exe
        .parent()
        .ok_or("compiler gate has no parent directory")?;
    let root = bin
        .ancestors()
        .nth(3)
        .ok_or("compiler gate is not installed under tools/compiler-lints/bin")?;
    let sysroot = Path::new(env!("FOGSCRIB_LINT_SYSROOT"));
    let source = root.join("tools").join("compiler-lints");
    if std::fs::read(source.join("driver.rs"))? != include_bytes!("driver.rs")
        || std::fs::read(source.join("policy.rs"))? != include_bytes!("policy.rs")
        || std::fs::read(source.join("wrapper.rs"))? != include_bytes!("wrapper.rs")
        || std::fs::read(root.join("rust-toolchain.toml"))?
            != include_bytes!("../../rust-toolchain.toml")
    {
        return Err("compiler policy changed; run python tools/compiler-lints/bootstrap.py".into());
    }
    let mut args: Vec<OsString> = env::args_os().skip(1).collect();
    let mut wrapped = false;
    let mut clippy = false;
    while let Some(first) = args.first() {
        match Path::new(first).file_stem().and_then(|stem| stem.to_str()) {
            Some("rustc") => wrapped = true,
            Some("clippy-driver") => {
                wrapped = true;
                clippy = true;
            }
            _ => break,
        }
        args.remove(0);
    }
    let source = args.iter().find_map(|arg| {
        let path = Path::new(arg);
        path.is_file().then(|| path.canonicalize().ok()).flatten()
    });
    let manifest =
        env::var_os("CARGO_MANIFEST_DIR").and_then(|path| PathBuf::from(path).canonicalize().ok());
    let owned = !wrapped
        || source.as_ref().is_some_and(|path| path.starts_with(root))
        || manifest.as_ref().is_some_and(|path| path.starts_with(root));
    let compilation = !args.is_empty();
    let probe = args.iter().any(|arg| {
        arg.to_str().is_some_and(|arg| {
            arg == "-vV"
                || arg == "-V"
                || arg == "--version"
                || (source.is_none()
                    && arg.starts_with("--print")
                    && !args.iter().any(|arg| {
                        let arg = arg.to_string_lossy();
                        arg.contains("link-args") || arg.contains("native-static-libs")
                    }))
        })
    });
    if args.iter().any(|arg| arg == "-vV") {
        let mut fingerprint = std::collections::hash_map::DefaultHasher::new();
        include_bytes!("driver.rs").hash(&mut fingerprint);
        include_bytes!("policy.rs").hash(&mut fingerprint);
        include_bytes!("wrapper.rs").hash(&mut fingerprint);
        std::fs::read(root.join("clippy.toml"))?.hash(&mut fingerprint);
        println!(
            "{}\nfogscrib-policy: {:016x}",
            env!("FOGSCRIB_LINT_IDENTITY"),
            fingerprint.finish()
        );
        return Ok(0);
    }
    if owned && compilation && !probe {
        let mut uncapped = Vec::new();
        let mut arguments = args.into_iter();
        while let Some(argument) = arguments.next() {
            if argument == "--cap-lints" {
                arguments.next().ok_or("--cap-lints has no value")?;
            } else if !argument.to_string_lossy().starts_with("--cap-lints=") {
                uncapped.push(argument);
            }
        }
        args = uncapped;
    }
    let mut paths = vec![
        sysroot.join("bin"),
        sysroot.join("lib"),
        sysroot
            .join("lib")
            .join("rustlib")
            .join(env!("FOGSCRIB_LINT_HOST"))
            .join("lib"),
    ];
    paths.extend(env::split_paths(&env::var_os("PATH").unwrap_or_default()));
    let path = env::join_paths(paths)?;
    let library_path = env::join_paths([sysroot.join("lib"), sysroot.join("bin")])?;
    if owned && compilation && !probe {
        let mut analysis = Command::new(bin.join(program("fogscrib-lint-driver")));
        analysis
            .args(&args)
            .env("PATH", &path)
            .env("LD_LIBRARY_PATH", &library_path)
            .env("DYLD_LIBRARY_PATH", &library_path);
        if !args
            .iter()
            .any(|arg| arg.to_string_lossy().starts_with("--sysroot"))
        {
            analysis.arg("--sysroot").arg(sysroot);
        }
        let status = analysis.status()?;
        if !status.success() {
            return Ok(status.code().unwrap_or(1));
        }
        clippy = true;
    }
    let compiler =
        sysroot
            .join("bin")
            .join(program(if clippy { "clippy-driver" } else { "rustc" }));
    let mut command = Command::new(compiler);
    command.args(&args).env("PATH", path);
    if owned && compilation && !probe {
        let configuration = match env::var_os("CLIPPY_CONF_DIR") {
            Some(configuration) => {
                if Path::new(&configuration).canonicalize()? != root {
                    return Err(
                        "CLIPPY_CONF_DIR must select the compiler gate's repository root".into(),
                    );
                }
                configuration
            }
            None if wrapped && env::var_os("CARGO_MANIFEST_DIR").is_some() => {
                return Err(
                    "Cargo must inherit CLIPPY_CONF_DIR from the repository .cargo/config.toml; run Cargo from the repository or an owned descendant"
                        .into(),
                );
            }
            None => root.as_os_str().to_owned(),
        };
        command
            .env("CLIPPY_CONF_DIR", configuration)
            // Denied, not forbidden: a forbid turns the `allow(clippy::style)`
            // that derive macros emit into an error. The HIR pass rejects
            // hand-written allows of these lints instead (`lint_override`).
            .arg("--deny=clippy::disallowed_methods")
            .arg("--deny=clippy::disallowed_types");
    }
    let status = command.status()?;
    Ok(status.code().unwrap_or(1))
}

fn main() {
    match run() {
        Ok(code) => process::exit(code),
        Err(error) => {
            eprintln!("fogscrib compiler gate: {error}");
            process::exit(1);
        }
    }
}
