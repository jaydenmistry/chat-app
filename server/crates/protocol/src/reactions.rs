//! The reactions the app offers. A fixed list: nothing to validate beyond
//! membership in it, and a bounded number of distinct reactions per message.
//!
//! The web client gets the same list as generated TypeScript
//! (`reactions.ts`), written by the test below, so the two can't drift.

pub const ALLOWED: [&str; 24] = [
    "👍", "👎", "😂", "❤️", "🎉", "😮", "😢", "😡", "🙏", "👀", "🔥", "✅", "❌", "💯", "🚀", "🤔",
    "👏", "😅", "🙌", "💡", "⭐", "🤝", "😍", "🫡",
];

pub fn is_allowed(emoji: &str) -> bool {
    ALLOWED.contains(&emoji)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allowlist() {
        assert!(is_allowed("👍"));
        assert!(is_allowed("❤️"));
        assert!(
            !is_allowed("❤"),
            "the bare heart without the emoji variation selector"
        );
        assert!(!is_allowed("<script>"));
        assert!(!is_allowed(""));
        let mut unique = ALLOWED.to_vec();
        unique.sort_unstable();
        unique.dedup();
        assert_eq!(unique.len(), ALLOWED.len());
    }

    /// Writes the list for the web client next to the ts-rs output. CI
    /// regenerates everything and fails if the committed files differ.
    #[test]
    fn export_reactions_for_typescript() {
        let dir = std::env::var("TS_RS_EXPORT_DIR").unwrap_or_else(|_| "./bindings".into());
        let items: Vec<String> = ALLOWED.iter().map(|e| format!("  \"{e}\",")).collect();
        let ts = format!(
            "// Generated from server/crates/protocol/src/reactions.rs. Do not edit.\n\nexport const REACTIONS = [\n{}\n] as const;\n",
            items.join("\n")
        );
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(format!("{dir}/reactions.ts"), ts).unwrap();
    }
}
