//! Verifying a project's tokens, the way the pinned server does, because the client shows its
//! refusals word for word.
//!
//! What has to match:
//!  - **the algorithms**: HS256, HS384 and HS512 with the project's secret. Our projects sign
//!    HS256 with their own secret; asymmetric keys (JWKS) are not supported.
//!  - **`exp` must be in the future**, and `role` and `exp` must both be present. A NumericDate
//!    may carry a fraction (RFC 7519); it is accepted and rounded.
//!  - **the sentences**: "The token provided is not a valid JWT", "Token has expired N seconds
//!    ago", "Fields `role` and `exp` are required in JWT", and the signature failure.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use hmac::{Hmac, Mac};
use serde_json::{Map, Value};
use sha2::{Sha256, Sha384, Sha512};

/// Why a token was refused. Each maps to the code and sentence the client sees.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TokenError {
	/// Not three base64url JSON parts.
	Malformed,
	/// The header is not a JSON object, or names an algorithm we do not verify.
	Signer,
	/// The signature does not verify.
	Signature,
	/// `exp` is in the past: how many seconds ago.
	Expired(i64),
	/// `role` or `exp` is missing.
	MissingClaims,
}

impl TokenError {
	/// The code and the sentence, joined as the pinned server joins them for a join refusal.
	pub fn reason(&self) -> String {
		match self {
			TokenError::Malformed => "MalformedJWT: The token provided is not a valid JWT".into(),
			TokenError::Signer => "JwtSignerError: Failed to generate JWT signer, check your JWT secret or JWKS configuration".into(),
			TokenError::Signature => "JwtSignatureError: Failed to validate JWT signature".into(),
			TokenError::Expired(ago) => format!("InvalidJWTToken: Token has expired {ago} seconds ago"),
			TokenError::MissingClaims => "InvalidJWTToken: Fields `role` and `exp` are required in JWT".into(),
		}
	}

	/// The sentence alone, as a channel shutdown carries it.
	pub fn message(&self) -> String {
		match self {
			TokenError::Expired(ago) => format!("Token has expired {ago} seconds ago"),
			TokenError::MissingClaims => "Fields `role` and `exp` are required in JWT".into(),
			TokenError::Malformed => "The token provided is not a valid JWT".into(),
			other => other.reason(),
		}
	}
}

fn decode_json(part: &str) -> Option<Value> {
	let bytes = URL_SAFE_NO_PAD.decode(part.trim_end_matches('=')).ok()?;
	serde_json::from_slice(&bytes).ok()
}

fn mac(alg: &str, key: &[u8], data: &[u8]) -> Option<Vec<u8>> {
	macro_rules! run {
		($hash:ty) => {{
			let mut m = Hmac::<$hash>::new_from_slice(key).ok()?;
			m.update(data);
			Some(m.finalize().into_bytes().to_vec())
		}};
	}
	match alg {
		"HS256" => run!(Sha256),
		"HS384" => run!(Sha384),
		"HS512" => run!(Sha512),
		_ => None,
	}
}

/// Tidy a token the way the pinned server does before reading it: percent-decoded, and any
/// whitespace removed.
pub fn clean(token: &str) -> String {
	let decoded = percent_encoding::percent_decode_str(token).decode_utf8_lossy();
	decoded.chars().filter(|c| !c.is_whitespace()).collect()
}

/// Verify a token's signature with the project's secret and return its claims, with `exp`
/// and `iat` rounded to whole seconds. `now` is seconds since the epoch.
pub fn verify(token: &str, secret: &str, now: i64) -> Result<Map<String, Value>, TokenError> {
	let token = clean(token);
	let mut parts = token.split('.');
	let (Some(head), Some(body), Some(signature), None) =
		(parts.next(), parts.next(), parts.next(), parts.next())
	else {
		return Err(TokenError::Malformed);
	};
	let Some(Value::Object(claims)) = decode_json(body) else {
		return Err(TokenError::Malformed);
	};
	let Some(Value::Object(header)) = decode_json(head) else {
		return Err(TokenError::Signer);
	};
	let alg = header
		.get("alg")
		.and_then(Value::as_str)
		.unwrap_or_default();
	let Some(expected) = mac(alg, secret.as_bytes(), format!("{head}.{body}").as_bytes()) else {
		return Err(TokenError::Signer);
	};
	let given = URL_SAFE_NO_PAD
		.decode(signature.trim_end_matches('='))
		.map_err(|_| TokenError::Signature)?;
	if !constant_time_eq(&expected, &given) {
		return Err(TokenError::Signature);
	}
	let mut claims = claims;
	match claims.get("exp").and_then(Value::as_f64) {
		Some(exp) if exp > now as f64 => {}
		Some(exp) => return Err(TokenError::Expired(now - exp.round() as i64)),
		None if claims.contains_key("exp") => return Err(TokenError::Expired(0)),
		None => {}
	}
	for key in ["exp", "iat"] {
		if let Some(n) = claims.get(key).and_then(Value::as_f64)
			&& claims.get(key).and_then(Value::as_i64).is_none()
		{
			claims.insert(key.into(), Value::from(n.round() as i64));
		}
	}
	Ok(claims)
}

/// `verify`, then the two claims a connection needs.
pub fn authorize(token: &str, secret: &str, now: i64) -> Result<Map<String, Value>, TokenError> {
	let claims = verify(token, secret, now)?;
	if claims.contains_key("role") && claims.contains_key("exp") {
		Ok(claims)
	} else {
		Err(TokenError::MissingClaims)
	}
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
	a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// Sign claims HS256; the admin API's tests and the metrics reader's tests use it.
pub fn sign_hs256(claims: &Value, secret: &str) -> String {
	let head = URL_SAFE_NO_PAD.encode(br#"{"alg":"HS256","typ":"JWT"}"#);
	let body = URL_SAFE_NO_PAD.encode(claims.to_string());
	let signature = mac(
		"HS256",
		secret.as_bytes(),
		format!("{head}.{body}").as_bytes(),
	)
	.unwrap_or_default();
	format!("{head}.{body}.{}", URL_SAFE_NO_PAD.encode(signature))
}

#[cfg(test)]
mod tests {
	use super::*;
	use serde_json::json;

	const SECRET: &str = "a-test-secret-that-is-long-enough";

	#[test]
	fn a_good_token_verifies() {
		let t = sign_hs256(&json!({"role": "anon", "exp": 2000}), SECRET);
		assert_eq!(authorize(&t, SECRET, 1000).unwrap()["role"], "anon");
	}

	#[test]
	fn fractional_dates_are_accepted_and_rounded() {
		let t = sign_hs256(
			&json!({"role": "anon", "exp": 2000.5, "iat": 999.75}),
			SECRET,
		);
		let claims = authorize(&t, SECRET, 1000).unwrap();
		assert_eq!(claims["exp"], json!(2001));
		assert_eq!(claims["iat"], json!(1000));
	}

	#[test]
	fn refusals_say_what_the_client_shows() {
		assert_eq!(
			authorize("not-a-jwt", SECRET, 0).unwrap_err().reason(),
			"MalformedJWT: The token provided is not a valid JWT"
		);
		let expired = sign_hs256(&json!({"role": "anon", "exp": 900}), SECRET);
		assert_eq!(
			authorize(&expired, SECRET, 1000).unwrap_err().message(),
			"Token has expired 100 seconds ago"
		);
		let no_role = sign_hs256(&json!({"exp": 2000}), SECRET);
		assert_eq!(
			authorize(&no_role, SECRET, 1000).unwrap_err(),
			TokenError::MissingClaims
		);
		let other = sign_hs256(
			&json!({"role": "anon", "exp": 2000}),
			"a-different-secret-entirely",
		);
		assert_eq!(
			authorize(&other, SECRET, 1000).unwrap_err(),
			TokenError::Signature
		);
	}

	#[test]
	fn whitespace_and_percent_encoding_are_tidied() {
		let t = sign_hs256(&json!({"role": "anon", "exp": 2000}), SECRET);
		let spaced = format!(" {}\n", t.replace('.', "%2E"));
		assert!(authorize(&spaced, SECRET, 1000).is_ok());
	}
}
