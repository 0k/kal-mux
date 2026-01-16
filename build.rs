use std::{env, fs, path::PathBuf};

fn main() {
    println!("cargo:rerun-if-changed=README.org");
    let org = fs::read_to_string("README.org").expect("read README.org");
    let mut out = String::new();
    let mut in_block = false;
    let mut lang: String;

    for line in org.lines() {
        if let Some(rest) = line.strip_prefix("#+begin_src") {
            in_block = true;
            lang = rest.trim().to_string(); // e.g. "rust"
            out.push_str("```");
            if !lang.is_empty() {
                out.push_str(&lang);
            }
            out.push('\n');
        } else if line.trim() == "#+end_src" && in_block {
            in_block = false;
            out.push_str("```\n");
        } else {
            out.push_str(line);
            out.push('\n');
        }
    }

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap()).join("README.md");
    fs::write(&out_path, out).expect("write README.md");
}
