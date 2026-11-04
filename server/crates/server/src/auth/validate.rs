//! Input validation for account fields. Each function returns the normalized
//! value or an `invalid_input` error naming the field.

use crate::http::error::AppError;

pub const HANDLE_MIN: usize = 3;
pub const HANDLE_MAX: usize = 32;
pub const DISPLAY_NAME_MAX: usize = 64;
pub const PASSWORD_MIN: usize = 8;
pub const PASSWORD_MAX: usize = 128;

/// Handles starting with this belong to guest accounts.
pub const GUEST_PREFIX: &str = "guest_";

/// Lowercased and trimmed. Deliberately loose: without verification, the
/// only real test of an email is sending to it, which v1 doesn't do.
pub fn email(raw: &str) -> Result<String, AppError> {
    let email = raw.trim().to_lowercase();
    let well_formed = email.len() <= 254
        && !email.contains(char::is_whitespace)
        && email.split_once('@').is_some_and(|(local, domain)| {
            !local.is_empty() && domain.contains('.') && !domain.contains('@')
        });
    if well_formed {
        Ok(email)
    } else {
        Err(AppError::invalid("email", "enter a valid email address"))
    }
}

/// Length only, counted in characters. No composition rules, following NIST
/// SP 800-63B; the upper bound keeps hashing input reasonable.
pub fn password(raw: &str) -> Result<(), AppError> {
    let len = raw.chars().count();
    if len < PASSWORD_MIN {
        Err(AppError::invalid(
            "password",
            format!("use at least {PASSWORD_MIN} characters"),
        ))
    } else if len > PASSWORD_MAX {
        Err(AppError::invalid(
            "password",
            format!("use at most {PASSWORD_MAX} characters"),
        ))
    } else {
        Ok(())
    }
}

/// Lowercase letters, digits and underscores, 3 to 32 long.
pub fn handle(raw: &str) -> Result<String, AppError> {
    let handle = raw.trim().to_lowercase();
    if !is_valid_handle(&handle) {
        return Err(AppError::invalid(
            "handle",
            format!("use {HANDLE_MIN} to {HANDLE_MAX} letters, digits or underscores"),
        ));
    }
    if handle.starts_with(GUEST_PREFIX) {
        return Err(AppError::invalid(
            "handle",
            "handles starting with guest_ are reserved",
        ));
    }
    Ok(handle)
}

pub fn is_valid_handle(handle: &str) -> bool {
    (HANDLE_MIN..=HANDLE_MAX).contains(&handle.len())
        && handle
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}

/// Trimmed, 1 to 64 characters, no control characters.
pub fn display_name(raw: &str) -> Result<String, AppError> {
    let name = raw.trim();
    let len = name.chars().count();
    if len == 0 || len > DISPLAY_NAME_MAX || name.chars().any(char::is_control) {
        return Err(AppError::invalid(
            "display_name",
            format!("use 1 to {DISPLAY_NAME_MAX} characters"),
        ));
    }
    Ok(name.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn field(result: Result<impl std::fmt::Debug, AppError>) -> &'static str {
        match result {
            Err(AppError::Invalid { field, .. }) => field,
            other => panic!("expected invalid input, got {other:?}"),
        }
    }

    #[test]
    fn emails_are_normalized() {
        assert_eq!(email("  Ada@Example.COM ").unwrap(), "ada@example.com");
        for bad in [
            "",
            "ada",
            "@example.com",
            "ada@localhost",
            "a b@example.com",
            "a@b@c.com",
        ] {
            assert_eq!(field(email(bad)), "email", "{bad:?}");
        }
    }

    #[test]
    fn password_length_counts_characters() {
        assert!(password("12345678").is_ok());
        assert!(password("éééééééé").is_ok());
        assert_eq!(field(password("1234567")), "password");
        assert_eq!(field(password(&"x".repeat(129))), "password");
    }

    #[test]
    fn handles() {
        assert_eq!(handle(" Ada_99 ").unwrap(), "ada_99");
        for bad in [
            "ab",
            "has-dash",
            "has space",
            &"x".repeat(33),
            "guest_abc",
            "émile",
        ] {
            assert_eq!(field(handle(bad)), "handle", "{bad:?}");
        }
    }

    #[test]
    fn display_names() {
        assert_eq!(display_name("  Ada Lovelace ").unwrap(), "Ada Lovelace");
        assert!(display_name(&"é".repeat(64)).is_ok());
        for bad in ["", "   ", "tab\there", &"x".repeat(65)] {
            assert_eq!(field(display_name(bad)), "display_name", "{bad:?}");
        }
    }
}
