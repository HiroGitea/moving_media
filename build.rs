use std::path::Path;

fn main() {
    let font_path = "assets/NotoEmoji-Regular.ttf";

    if !Path::new(font_path).exists() {
        std::fs::create_dir_all("assets").expect("failed to create assets dir");

        let url = "https://raw.githubusercontent.com/googlefonts/noto-emoji/v2020-09-16-unicode13_1/fonts/NotoEmoji-Regular.ttf";
        let status = std::process::Command::new("curl")
            .args(["-fsSL", "-o", font_path, url])
            .status()
            .expect("curl not found; please install curl or manually place NotoEmoji-Regular.ttf in assets/");

        if !status.success() {
            panic!(
                "Failed to download NotoEmoji-Regular.ttf.\n\
                 Please download it manually:\n  curl -fsSL -o {font_path} '{url}'"
            );
        }

        println!("cargo:warning=Downloaded NotoEmoji-Regular.ttf to {font_path}");
    }

    println!("cargo:rerun-if-changed=assets/NotoEmoji-Regular.ttf");
}
