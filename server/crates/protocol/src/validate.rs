//! Rules for user-written text, shared by every endpoint that accepts it.

/// Message content: at most this many bytes of UTF-8, after trimming.
pub const MAX_CONTENT_BYTES: usize = 4096;
pub const MAX_ROOM_NAME_CHARS: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TextError {
    Empty,
    TooLong,
    /// Contains a control character other than newline or tab.
    ControlCharacter,
}

impl TextError {
    pub fn message(self) -> &'static str {
        match self {
            Self::Empty => "can't be empty",
            Self::TooLong => "is too long",
            Self::ControlCharacter => "contains a control character",
        }
    }
}

/// Trims the content and checks it. Newlines and tabs are allowed; other
/// control characters (C0 and C1, which include terminal escape sequences)
/// are not. Bidi overrides such as U+202E are format characters, not control
/// characters: they are allowed here, and the client renders every message
/// with `unicode-bidi: isolate` so they can't reorder text around it.
pub fn message_content(raw: &str) -> Result<&str, TextError> {
    let content = raw.trim();
    if content.is_empty() {
        return Err(TextError::Empty);
    }
    if content.len() > MAX_CONTENT_BYTES {
        return Err(TextError::TooLong);
    }
    if content
        .chars()
        .any(|c| c.is_control() && c != '\n' && c != '\t')
    {
        return Err(TextError::ControlCharacter);
    }
    Ok(content)
}

/// Trimmed, 1 to 64 characters, no control characters at all.
pub fn room_name(raw: &str) -> Result<&str, TextError> {
    let name = raw.trim();
    if name.is_empty() {
        return Err(TextError::Empty);
    }
    if name.chars().count() > MAX_ROOM_NAME_CHARS {
        return Err(TextError::TooLong);
    }
    if name.chars().any(char::is_control) {
        return Err(TextError::ControlCharacter);
    }
    Ok(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_is_trimmed_and_bounded_in_bytes() {
        assert_eq!(message_content("  hi\n there\t "), Ok("hi\n there"));
        assert_eq!(message_content(" \n "), Err(TextError::Empty));
        assert!(message_content(&"a".repeat(4096)).is_ok());
        assert_eq!(message_content(&"a".repeat(4097)), Err(TextError::TooLong));
        // 1366 three-byte characters are 4098 bytes.
        assert_eq!(message_content(&"€".repeat(1366)), Err(TextError::TooLong));
    }

    #[test]
    fn control_characters_are_rejected() {
        for bad in [
            "a\u{0}b",
            "bell\u{7}",
            "esc\u{1b}[31m",
            "c1\u{85}inside",
            "cr\rlf",
        ] {
            assert_eq!(
                message_content(bad),
                Err(TextError::ControlCharacter),
                "{bad:?}"
            );
        }
        // Trimming treats U+0085 (NEL) as whitespace, so it can't survive at
        // the ends.
        assert_eq!(message_content("c1\u{85}"), Ok("c1"));
        assert_eq!(
            message_content("rtl \u{202e}override"),
            Ok("rtl \u{202e}override")
        );
    }

    #[test]
    fn room_names() {
        assert_eq!(room_name("  General  "), Ok("General"));
        assert_eq!(room_name(""), Err(TextError::Empty));
        assert!(room_name(&"é".repeat(64)).is_ok());
        assert_eq!(room_name(&"é".repeat(65)), Err(TextError::TooLong));
        assert_eq!(room_name("tab\there"), Err(TextError::ControlCharacter));
    }
}
