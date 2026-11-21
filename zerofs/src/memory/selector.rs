use super::detect::{DetectedLimits, FiniteLimit};
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

/// An explicit operator selection. Deserialization validates its shape and units.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "SelectorFields", into = "SelectorFields")]
pub enum MemorySelector {
    Total(u64),
    FractionOfLimit(f64),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SelectorFields {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    total: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    fraction_of_limit: Option<f64>,
}

impl TryFrom<SelectorFields> for MemorySelector {
    type Error = anyhow::Error;

    fn try_from(fields: SelectorFields) -> Result<Self> {
        match (fields.total, fields.fraction_of_limit) {
            (Some(total), None) => Ok(Self::Total(parse_bytes(&total)?)),
            (None, Some(fraction)) => {
                validate_fraction(fraction)?;
                Ok(Self::FractionOfLimit(fraction))
            }
            _ => bail!("[memory] requires exactly one of total or fraction_of_limit"),
        }
    }
}

impl From<MemorySelector> for SelectorFields {
    fn from(selector: MemorySelector) -> Self {
        match selector {
            MemorySelector::Total(bytes) => Self {
                total: Some(format!("{bytes} B")),
                fraction_of_limit: None,
            },
            MemorySelector::FractionOfLimit(fraction) => Self {
                total: None,
                fraction_of_limit: Some(fraction),
            },
        }
    }
}

fn parse_bytes(raw: &str) -> Result<u64> {
    let raw = raw.trim();
    let digits = raw.bytes().take_while(u8::is_ascii_digit).count();
    let (number, unit) = raw.split_at(digits);
    let quantity: u64 = number
        .parse()
        .context("[memory] total requires an unsigned integer byte quantity")?;
    let multiplier = match unit.trim() {
        "B" => 1,
        "KiB" => 1024,
        "MiB" => 1024 * 1024,
        "GiB" => 1024 * 1024 * 1024,
        "TiB" => 1024_u64.pow(4),
        "kB" => 1000,
        "MB" => 1000 * 1000,
        "GB" => 1000 * 1000 * 1000,
        "TB" => 1000_u64.pow(4),
        _ => bail!("[memory] total has an unknown unit in {raw:?}"),
    };
    let bytes = quantity
        .checked_mul(multiplier)
        .context("[memory] total overflowed")?;
    if bytes == 0 {
        bail!("[memory] total must be positive");
    }
    Ok(bytes)
}

fn validate_fraction(fraction: f64) -> Result<()> {
    if !fraction.is_finite() || fraction <= 0.0 || fraction > 1.0 {
        bail!("[memory] fraction_of_limit must be finite, greater than zero and at most one");
    }
    Ok(())
}

/// The basis remains in the receipt even when it is shared or host-wide.
#[derive(Clone, Debug, Serialize)]
pub struct SelectedBudget {
    pub bytes: u64,
    pub selector: MemorySelector,
    pub basis: Option<FiniteLimit>,
    pub envelope_validation: String,
}

impl MemorySelector {
    pub fn resolve(&self, detected: &DetectedLimits) -> Result<SelectedBudget> {
        let basis = detected.effective_limit();
        let bytes = match *self {
            Self::Total(bytes) => bytes,
            Self::FractionOfLimit(fraction) => {
                validate_fraction(fraction)?;
                let limit = basis
                    .as_ref()
                    .context("[memory] fraction_of_limit needs an available detected limit")?;
                // Multiply the exact binary fraction using integers. Converting the
                // limit to f64 first can round upward near u64::MAX.
                let bits = fraction.to_bits();
                let exponent = ((bits >> 52) & 0x7ff) as u32;
                let mantissa = if exponent == 0 {
                    bits & ((1_u64 << 52) - 1)
                } else {
                    (bits & ((1_u64 << 52) - 1)) | (1_u64 << 52)
                };
                let shift = if exponent == 0 { 1074 } else { 1075 - exponent };
                let product = u128::from(limit.bytes) * u128::from(mantissa);
                let rounded = product.checked_shr(shift).unwrap_or(0);
                u64::try_from(rounded).context("[memory] fraction overflowed")?
            }
        };
        if bytes == 0 {
            bail!("[memory] budget rounds to zero bytes");
        }
        if let Some(limit) = &basis
            && bytes > limit.bytes
        {
            let policy = if limit.kind == super::detect::LimitKind::High {
                "the operational memory.high policy threshold"
            } else {
                "the visible capacity ceiling"
            };
            bail!(
                "[memory] budget {bytes} bytes exceeds {policy} of {} bytes at {}",
                limit.bytes,
                limit.source.display()
            );
        }
        Ok(SelectedBudget {
            bytes,
            selector: self.clone(),
            envelope_validation: if basis.is_some() {
                "checked against available observations"
            } else {
                "unavailable; physical and kernel-envelope validation could not be checked"
            }
            .into(),
            basis,
        })
    }
}
