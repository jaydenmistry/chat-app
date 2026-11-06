//! Configuration, read once at startup from environment variables.
//!
//! [`Config::from_env`] validates every variable and reports every problem at
//! once, so a misconfigured deployment fails with one complete list instead of
//! one error per restart. Every variable is listed in `.env.example`.

use std::{
    collections::HashMap, fmt, net::SocketAddr, path::PathBuf, str::FromStr, time::Duration,
};

use crate::rate_limit::{Bucket, RateLimits};

#[derive(Debug, Clone)]
pub struct Config {
    /// Where the HTTP server listens (`BIND_ADDR`).
    pub bind_addr: SocketAddr,
    /// The origin users load the app from, for example `https://chat.jmistry.com`
    /// (`PUBLIC_URL`). Used for the GitHub redirect URL and as the default
    /// allowed origin.
    pub public_url: String,
    /// Origins allowed to make state-changing requests (`ALLOWED_ORIGINS`).
    pub allowed_origins: Vec<String>,
    /// Header that carries the client IP when behind a trusted proxy
    /// (`CLIENT_IP_HEADER`). When unset, the TCP peer address is used.
    pub client_ip_header: Option<String>,
    pub log_format: LogFormat,
    /// Built web client to serve at `/` (`STATIC_DIR`). Unset in development,
    /// where Vite serves the client.
    pub static_dir: Option<PathBuf>,
    pub database_url: Secret,
    pub database_max_connections: u32,
    pub redis_url: Secret,
    pub scylla_nodes: Vec<String>,
    pub scylla_keyspace: String,
    pub scylla_replication_factor: u8,
    /// `None` when GitHub sign-in is not configured.
    pub github: Option<GithubConfig>,
    pub session_ttl: Duration,
    pub guest_ttl: Duration,
    /// Most rooms a user may be a member of (`MAX_ROOMS_PER_USER`), and the
    /// lower cap for guests (`MAX_ROOMS_PER_GUEST`).
    pub max_rooms_per_user: i64,
    pub max_rooms_per_guest: i64,
    /// Most WebSocket connections one user, and one client IP, may hold open
    /// at once across all instances (`MAX_SOCKETS_PER_USER`,
    /// `MAX_SOCKETS_PER_IP`; docs/design.md 11.2).
    pub max_sockets_per_user: i64,
    pub max_sockets_per_ip: i64,
    /// Every rate limit, built from the `RATE_*` variables (docs/design.md
    /// 11.1).
    pub rate_limits: RateLimits,
    /// Worker-ID lease timings. Not configurable by environment: the
    /// defaults are the design. Tests shorten the quarantine.
    pub id_lease: crate::ids::LeaseTimings,
    /// Shifts this instance's clock for ID generation. Always 0 outside tests,
    /// which use it to run instances with different clocks.
    pub clock_offset_ms: i64,
}

#[derive(Debug, Clone)]
pub struct GithubConfig {
    pub client_id: String,
    pub client_secret: Secret,
    /// `https://github.com` unless overridden, which only tests do.
    pub oauth_base_url: String,
    /// `https://api.github.com` unless overridden, which only tests do.
    pub api_base_url: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogFormat {
    Json,
    Pretty,
}

/// A string that never appears in `Debug` output, so configuration can be
/// logged without leaking credentials.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(String);

impl Secret {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret([redacted])")
    }
}

/// Every problem found in the environment, in the order the variables are read.
#[derive(Debug)]
pub struct ConfigError {
    pub problems: Vec<String>,
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "invalid configuration:")?;
        for problem in &self.problems {
            writeln!(f, "  - {problem}")?;
        }
        Ok(())
    }
}

impl std::error::Error for ConfigError {}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_vars(&std::env::vars().collect())
    }

    /// Parses configuration from a map, so tests never touch the process
    /// environment (which is shared by every test thread).
    pub fn from_vars(vars: &HashMap<String, String>) -> Result<Self, ConfigError> {
        let mut p = Parser {
            vars,
            problems: Vec::new(),
        };

        let bind_addr = p.parse_or("BIND_ADDR", SocketAddr::from(([0, 0, 0, 0], 8080)));
        let public_url = p
            .required("PUBLIC_URL")
            .and_then(|url| p.origin("PUBLIC_URL", &url));
        let allowed_origins = match p.optional("ALLOWED_ORIGINS") {
            Some(list) => list
                .split(',')
                .filter_map(|o| p.origin("ALLOWED_ORIGINS", o.trim()))
                .collect(),
            None => public_url.iter().cloned().collect(),
        };
        let client_ip_header = p.optional("CLIENT_IP_HEADER").filter(|name| {
            let valid = name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-');
            if !valid {
                p.problem(format!(
                    "CLIENT_IP_HEADER: `{name}` is not a valid header name"
                ));
            }
            valid
        });
        let log_format = match p.optional("LOG_FORMAT").as_deref() {
            None | Some("json") => LogFormat::Json,
            Some("pretty") => LogFormat::Pretty,
            Some(other) => {
                p.problem(format!(
                    "LOG_FORMAT: expected `json` or `pretty`, got `{other}`"
                ));
                LogFormat::Json
            }
        };
        let static_dir = p.optional("STATIC_DIR").map(PathBuf::from).filter(|dir| {
            let exists = dir.is_dir();
            if !exists {
                p.problem(format!(
                    "STATIC_DIR: `{}` is not a directory",
                    dir.display()
                ));
            }
            exists
        });

        let database_url =
            p.required_with_prefix("DATABASE_URL", &["postgres://", "postgresql://"]);
        let database_max_connections = p.parse_in_range("DATABASE_MAX_CONNECTIONS", 10, 1..=1000);
        let redis_url = p.required_with_prefix("REDIS_URL", &["redis://", "rediss://"]);

        let scylla_nodes = p
            .required("SCYLLA_NODES")
            .map(|list| {
                list.split(',')
                    .map(str::trim)
                    .filter(|node| {
                        let valid = node.rsplit_once(':').is_some_and(|(host, port)| {
                            !host.is_empty() && port.parse::<u16>().is_ok()
                        });
                        if !valid {
                            p.problem(format!("SCYLLA_NODES: `{node}` is not host:port"));
                        }
                        valid
                    })
                    .map(String::from)
                    .collect()
            })
            .unwrap_or_default();
        let scylla_keyspace = p
            .optional("SCYLLA_KEYSPACE")
            .unwrap_or_else(|| "chat".into());
        // The keyspace name is interpolated into CQL, so it must be a plain
        // identifier. Scylla allows up to 48 characters.
        let keyspace_valid = scylla_keyspace.len() <= 48
            && scylla_keyspace.starts_with(|c: char| c.is_ascii_lowercase())
            && scylla_keyspace
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_');
        if !keyspace_valid {
            p.problem(format!(
                "SCYLLA_KEYSPACE: `{scylla_keyspace}` must match [a-z][a-z0-9_]{{0,47}}"
            ));
        }
        let scylla_replication_factor = p.parse_in_range("SCYLLA_REPLICATION_FACTOR", 1, 1..=9);

        let github = match (
            p.optional("GITHUB_CLIENT_ID"),
            p.optional("GITHUB_CLIENT_SECRET"),
        ) {
            (Some(client_id), Some(client_secret)) => Some(GithubConfig {
                client_id,
                client_secret: Secret(client_secret),
                oauth_base_url: p
                    .optional("GITHUB_OAUTH_BASE_URL")
                    .unwrap_or_else(|| "https://github.com".into()),
                api_base_url: p
                    .optional("GITHUB_API_BASE_URL")
                    .unwrap_or_else(|| "https://api.github.com".into()),
            }),
            (None, None) => None,
            _ => {
                p.problem(
                    "GITHUB_CLIENT_ID and GITHUB_CLIENT_SECRET must be set together, or both left unset"
                        .into(),
                );
                None
            }
        };

        let session_ttl_days: u64 = p.parse_in_range("SESSION_TTL_DAYS", 30, 1..=365);
        let guest_ttl_hours: u64 = p.parse_in_range("GUEST_TTL_HOURS", 24, 1..=168);
        let max_rooms_per_user = p.parse_in_range("MAX_ROOMS_PER_USER", 100, 1..=1_000_000);
        let max_rooms_per_guest = p.parse_in_range("MAX_ROOMS_PER_GUEST", 20, 1..=1_000_000);
        let max_sockets_per_user = p.parse_in_range("MAX_SOCKETS_PER_USER", 5, 1..=1_000_000);
        let max_sockets_per_ip = p.parse_in_range("MAX_SOCKETS_PER_IP", 20, 1..=1_000_000);

        // Starting values from docs/design.md 11.1. Each is `RATE_<NAME>` =
        // `<capacity>,<refill_per_second>`.
        let rate_limits = RateLimits {
            send_user: p.bucket("RATE_SEND_USER", Bucket::per_second(10.0, 1.0)),
            send_ip: p.bucket("RATE_SEND_IP", Bucket::per_second(30.0, 3.0)),
            send_room: p.bucket("RATE_SEND_ROOM", Bucket::per_second(50.0, 10.0)),
            mutate_user: p.bucket("RATE_MUTATE_USER", Bucket::per_second(20.0, 2.0)),
            signup_ip: p.bucket("RATE_SIGNUP_IP", Bucket::per_period(5.0, 3600.0)),
            guest_ip: p.bucket("RATE_GUEST_IP", Bucket::per_period(10.0, 3600.0)),
            login_ip: p.bucket("RATE_LOGIN_IP", Bucket::per_period(20.0, 600.0)),
            login_account: p.bucket("RATE_LOGIN_ACCOUNT", Bucket::per_period(10.0, 600.0)),
            room_create_user: p.bucket("RATE_ROOM_CREATE_USER", Bucket::per_period(10.0, 86400.0)),
            join_user: p.bucket("RATE_JOIN_USER", Bucket::per_period(30.0, 3600.0)),
            join_guest: p.bucket("RATE_JOIN_GUEST", Bucket::per_period(10.0, 3600.0)),
        };

        if !p.problems.is_empty() {
            return Err(ConfigError {
                problems: p.problems,
            });
        }

        // Every `None` below was recorded as a problem above, so these cannot
        // fail once the problem list is empty.
        Ok(Config {
            bind_addr,
            public_url: public_url.expect("checked above"),
            allowed_origins,
            client_ip_header,
            log_format,
            static_dir,
            database_url: database_url.expect("checked above"),
            database_max_connections,
            redis_url: redis_url.expect("checked above"),
            scylla_nodes,
            scylla_keyspace,
            scylla_replication_factor,
            github,
            session_ttl: Duration::from_secs(session_ttl_days * 24 * 3600),
            guest_ttl: Duration::from_secs(guest_ttl_hours * 3600),
            max_rooms_per_user,
            max_rooms_per_guest,
            max_sockets_per_user,
            max_sockets_per_ip,
            rate_limits,
            id_lease: crate::ids::LeaseTimings::default(),
            clock_offset_ms: 0,
        })
    }

    /// Where GitHub sends the browser back after sign-in.
    pub fn github_redirect_url(&self) -> String {
        format!("{}/api/auth/github/callback", self.public_url)
    }
}

/// Reads variables and collects problems instead of stopping at the first.
struct Parser<'a> {
    vars: &'a HashMap<String, String>,
    problems: Vec<String>,
}

impl Parser<'_> {
    fn problem(&mut self, message: String) {
        self.problems.push(message);
    }

    /// A variable set to the empty string counts as unset.
    fn optional(&self, name: &str) -> Option<String> {
        self.vars
            .get(name)
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
    }

    fn required(&mut self, name: &str) -> Option<String> {
        let value = self.optional(name);
        if value.is_none() {
            self.problem(format!("{name}: required but not set"));
        }
        value
    }

    fn required_with_prefix(&mut self, name: &str, prefixes: &[&str]) -> Option<Secret> {
        let value = self.required(name)?;
        if prefixes.iter().any(|prefix| value.starts_with(prefix)) {
            Some(Secret(value))
        } else {
            // The value may contain a password, so it is not echoed.
            self.problem(format!("{name}: must start with {}", prefixes.join(" or ")));
            None
        }
    }

    fn parse_or<T: FromStr>(&mut self, name: &str, default: T) -> T {
        match self.optional(name) {
            None => default,
            Some(raw) => raw.parse().unwrap_or_else(|_| {
                self.problem(format!("{name}: `{raw}` is not valid"));
                default
            }),
        }
    }

    fn parse_in_range<T>(&mut self, name: &str, default: T, range: std::ops::RangeInclusive<T>) -> T
    where
        T: FromStr + PartialOrd + fmt::Display + Copy,
    {
        let value = self.parse_or(name, default);
        if !range.contains(&value) {
            self.problem(format!(
                "{name}: {value} is outside {}..={}",
                range.start(),
                range.end()
            ));
        }
        value
    }

    /// A rate-limit bucket from `<capacity>,<refill_per_second>`, both
    /// positive. Falls back to `default` and records a problem otherwise.
    fn bucket(&mut self, name: &str, default: Bucket) -> Bucket {
        let Some(raw) = self.optional(name) else {
            return default;
        };
        let parsed = raw.split_once(',').and_then(|(cap, refill)| {
            match (cap.trim().parse::<f64>(), refill.trim().parse::<f64>()) {
                (Ok(capacity), Ok(refill_per_sec)) if capacity > 0.0 && refill_per_sec > 0.0 => {
                    Some(Bucket {
                        capacity,
                        refill_per_sec,
                    })
                }
                _ => None,
            }
        });
        parsed.unwrap_or_else(|| {
            self.problem(format!(
                "{name}: expected `<capacity>,<refill_per_second>` with positive numbers, got `{raw}`"
            ));
            default
        })
    }

    /// Accepts `scheme://host[:port]` with no path, which is the exact form
    /// browsers send in the `Origin` header. A trailing slash is removed.
    fn origin(&mut self, name: &str, raw: &str) -> Option<String> {
        let value = raw.strip_suffix('/').unwrap_or(raw);
        let rest = value
            .strip_prefix("https://")
            .or_else(|| value.strip_prefix("http://"));
        match rest {
            Some(host) if !host.is_empty() && !host.contains(['/', '?', '#', ' ']) => {
                Some(value.to_string())
            }
            _ => {
                self.problem(format!(
                    "{name}: `{raw}` must be an origin like https://chat.example.com (no path)"
                ));
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vars(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn minimal() -> Vec<(&'static str, &'static str)> {
        vec![
            ("PUBLIC_URL", "https://chat.example.com"),
            ("DATABASE_URL", "postgres://chat:pw@localhost/chat"),
            ("REDIS_URL", "redis://localhost:6379"),
            ("SCYLLA_NODES", "localhost:9042"),
        ]
    }

    fn problems(pairs: &[(&str, &str)]) -> Vec<String> {
        Config::from_vars(&vars(pairs)).unwrap_err().problems
    }

    #[test]
    fn minimal_config_uses_defaults() {
        let config = Config::from_vars(&vars(&minimal())).unwrap();
        assert_eq!(config.bind_addr, "0.0.0.0:8080".parse().unwrap());
        assert_eq!(config.allowed_origins, ["https://chat.example.com"]);
        assert_eq!(config.log_format, LogFormat::Json);
        assert_eq!(config.database_max_connections, 10);
        assert_eq!(config.scylla_keyspace, "chat");
        assert_eq!(config.scylla_replication_factor, 1);
        assert!(config.github.is_none());
        assert_eq!(config.session_ttl, Duration::from_secs(30 * 24 * 3600));
        assert_eq!(config.guest_ttl, Duration::from_secs(24 * 3600));
        assert_eq!(
            config.github_redirect_url(),
            "https://chat.example.com/api/auth/github/callback"
        );
    }

    #[test]
    fn rate_limits_default_and_parse() {
        let config = Config::from_vars(&vars(&minimal())).unwrap();
        assert_eq!(config.max_rooms_per_user, 100);
        assert_eq!(config.max_rooms_per_guest, 20);
        assert_eq!(config.max_sockets_per_user, 5);
        assert_eq!(config.max_sockets_per_ip, 20);
        assert_eq!(config.rate_limits.send_user, Bucket::per_second(10.0, 1.0));
        // Five an hour.
        assert!((config.rate_limits.signup_ip.refill_per_sec - 5.0 / 3600.0).abs() < f64::EPSILON);

        let mut pairs = minimal();
        pairs.push(("RATE_SEND_USER", "4,0.5"));
        let config = Config::from_vars(&vars(&pairs)).unwrap();
        assert_eq!(config.rate_limits.send_user, Bucket::per_second(4.0, 0.5));
    }

    #[test]
    fn rejects_a_malformed_rate_limit() {
        let mut pairs = minimal();
        pairs.push(("RATE_SEND_USER", "10"));
        assert_eq!(
            problems(&pairs),
            [
                "RATE_SEND_USER: expected `<capacity>,<refill_per_second>` with positive numbers, got `10`"
            ]
        );
    }

    #[test]
    fn reports_every_missing_variable_at_once() {
        assert_eq!(
            problems(&[]),
            [
                "PUBLIC_URL: required but not set",
                "DATABASE_URL: required but not set",
                "REDIS_URL: required but not set",
                "SCYLLA_NODES: required but not set",
            ]
        );
    }

    #[test]
    fn empty_values_count_as_unset() {
        let mut pairs = minimal();
        pairs.push(("LOG_FORMAT", ""));
        pairs[0] = ("PUBLIC_URL", "  ");
        assert_eq!(problems(&pairs), ["PUBLIC_URL: required but not set"]);
    }

    #[test]
    fn rejects_bad_values_without_echoing_secrets() {
        let mut pairs = minimal();
        pairs.extend([
            ("DATABASE_URL", "mysql://user:hunter2@db/chat"),
            ("DATABASE_MAX_CONNECTIONS", "0"),
            ("SCYLLA_NODES", "localhost:9042,nohost"),
            ("SCYLLA_KEYSPACE", "Chat-Prod"),
            ("LOG_FORMAT", "xml"),
        ]);
        let found = problems(&pairs);
        assert_eq!(
            found,
            [
                "LOG_FORMAT: expected `json` or `pretty`, got `xml`",
                "DATABASE_URL: must start with postgres:// or postgresql://",
                "DATABASE_MAX_CONNECTIONS: 0 is outside 1..=1000",
                "SCYLLA_NODES: `nohost` is not host:port",
                "SCYLLA_KEYSPACE: `Chat-Prod` must match [a-z][a-z0-9_]{0,47}",
            ]
        );
        assert!(found.iter().all(|p| !p.contains("hunter2")));
    }

    #[test]
    fn origins_must_not_have_paths() {
        let mut pairs = minimal();
        pairs.push((
            "ALLOWED_ORIGINS",
            "https://a.example.com/, https://b.example.com/app",
        ));
        assert_eq!(
            problems(&pairs),
            [
                "ALLOWED_ORIGINS: `https://b.example.com/app` must be an origin like https://chat.example.com (no path)"
            ]
        );

        pairs.pop();
        pairs.push((
            "ALLOWED_ORIGINS",
            "https://a.example.com/, http://localhost:5173",
        ));
        let config = Config::from_vars(&vars(&pairs)).unwrap();
        assert_eq!(
            config.allowed_origins,
            ["https://a.example.com", "http://localhost:5173"]
        );
    }

    #[test]
    fn github_needs_both_id_and_secret() {
        let mut pairs = minimal();
        pairs.push(("GITHUB_CLIENT_ID", "abc"));
        assert_eq!(
            problems(&pairs),
            ["GITHUB_CLIENT_ID and GITHUB_CLIENT_SECRET must be set together, or both left unset"]
        );

        pairs.push(("GITHUB_CLIENT_SECRET", "shh"));
        let github = Config::from_vars(&vars(&pairs)).unwrap().github.unwrap();
        assert_eq!(github.client_id, "abc");
        assert_eq!(github.oauth_base_url, "https://github.com");
        assert_eq!(github.api_base_url, "https://api.github.com");
    }

    #[test]
    fn debug_output_redacts_secrets() {
        let mut pairs = minimal();
        pairs.extend([
            ("GITHUB_CLIENT_ID", "abc"),
            ("GITHUB_CLIENT_SECRET", "topsecret"),
        ]);
        let config = Config::from_vars(&vars(&pairs)).unwrap();
        let printed = format!("{config:?}");
        assert!(!printed.contains("topsecret"));
        assert!(!printed.contains("chat:pw"));
        assert!(printed.contains("Secret([redacted])"));
    }
}
