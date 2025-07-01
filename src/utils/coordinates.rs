use crate::error::{ProcessingError, Result};

/// Convert DMS (Degrees:Minutes:Seconds) format to decimal degrees
///
/// # Examples
/// ```
/// use ecad_processor::utils::dms_to_decimal;
///
/// let decimal = dms_to_decimal("50:30:15").unwrap();
/// assert!((decimal - 50.504167).abs() < 0.000001);
/// ```
pub fn dms_to_decimal(dms: &str) -> Result<f64> {
    let parts: Vec<&str> = dms.split(':').collect();

    if parts.len() != 3 {
        return Err(ProcessingError::InvalidCoordinate(format!(
            "Invalid DMS format: '{}'. Expected format: 'DD:MM:SS'",
            dms
        )));
    }

    // Check if the coordinate is negative (can be indicated by a minus sign anywhere)
    let is_negative = dms.starts_with('-');

    let degrees = parts[0].parse::<f64>().map_err(|_| {
        ProcessingError::InvalidCoordinate(format!("Invalid degrees value: '{}'", parts[0]))
    })?;

    let minutes = parts[1].parse::<f64>().map_err(|_| {
        ProcessingError::InvalidCoordinate(format!("Invalid minutes value: '{}'", parts[1]))
    })?;

    let seconds = parts[2].parse::<f64>().map_err(|_| {
        ProcessingError::InvalidCoordinate(format!("Invalid seconds value: '{}'", parts[2]))
    })?;

    // Validate ranges
    if !(0.0..60.0).contains(&minutes) {
        return Err(ProcessingError::InvalidCoordinate(format!(
            "Minutes must be between 0 and 60, got: {}",
            minutes
        )));
    }

    if !(0.0..60.0).contains(&seconds) {
        return Err(ProcessingError::InvalidCoordinate(format!(
            "Seconds must be between 0 and 60, got: {}",
            seconds
        )));
    }

    // Calculate decimal value
    let decimal_value = degrees.abs() + minutes / 60.0 + seconds / 3600.0;

    // Apply sign
    if is_negative {
        Ok(-decimal_value)
    } else {
        Ok(decimal_value)
    }
}

/// Convert decimal degrees to DMS format
pub fn decimal_to_dms(decimal: f64) -> String {
    let sign = if decimal < 0.0 { "-" } else { "" };
    let abs_decimal = decimal.abs();

    let degrees = abs_decimal.floor() as i32;
    let minutes_decimal = (abs_decimal - degrees as f64) * 60.0;
    let minutes = minutes_decimal.floor() as i32;
    let seconds = (minutes_decimal - minutes as f64) * 60.0;

    format!("{}{}:{:02}:{:05.2}", sign, degrees, minutes, seconds)
}

/// Parse coordinate that might be in DMS or decimal format
pub fn parse_coordinate(coord_str: &str) -> Result<f64> {
    let trimmed = coord_str.trim();

    // Check if it's already in decimal format
    if !trimmed.contains(':') {
        trimmed.parse::<f64>().map_err(|_| {
            ProcessingError::InvalidCoordinate(format!("Invalid coordinate value: '{}'", coord_str))
        })
    } else {
        dms_to_decimal(trimmed)
    }
}

/// Validate UK coordinate bounds
pub fn validate_uk_coordinates(latitude: f64, longitude: f64) -> Result<()> {
    if !(49.5..=61.0).contains(&latitude) {
        return Err(ProcessingError::InvalidCoordinate(format!(
            "Latitude {} is outside UK bounds [49.5, 61.0]",
            latitude
        )));
    }

    if !(-8.0..=2.0).contains(&longitude) {
        return Err(ProcessingError::InvalidCoordinate(format!(
            "Longitude {} is outside UK bounds [-8.0, 2.0]",
            longitude
        )));
    }

    Ok(())
}

/// Calculate the distance between two points using the Haversine formula
pub fn haversine_distance(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    const EARTH_RADIUS_KM: f64 = 6371.0;

    let lat1_rad = lat1.to_radians();
    let lat2_rad = lat2.to_radians();
    let delta_lat = (lat2 - lat1).to_radians();
    let delta_lon = (lon2 - lon1).to_radians();

    let a = (delta_lat / 2.0).sin().powi(2)
        + lat1_rad.cos() * lat2_rad.cos() * (delta_lon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().asin();

    EARTH_RADIUS_KM * c
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dms_to_decimal() {
        assert!((dms_to_decimal("50:30:15").unwrap() - 50.504167).abs() < 0.000001);
        assert!((dms_to_decimal("51:28:38").unwrap() - 51.477222).abs() < 0.000001);

        // -0:07:39 = -(7/60 + 39/3600) = -(0.116667 + 0.010833) = -0.1275
        let result = dms_to_decimal("-0:07:39").unwrap();
        let expected = -0.1275;
        println!(
            "Result: {}, Expected: {}, Diff: {}",
            result,
            expected,
            (result - expected).abs()
        );
        assert!((result - expected).abs() < 0.0001); // Slightly larger tolerance
    }

    #[test]
    fn test_invalid_dms_format() {
        assert!(dms_to_decimal("50:30").is_err());
        assert!(dms_to_decimal("50:70:15").is_err()); // Invalid minutes
        assert!(dms_to_decimal("50:30:70").is_err()); // Invalid seconds
    }

    #[test]
    fn test_decimal_to_dms() {
        assert_eq!(decimal_to_dms(50.504167), "50:30:15.00");
        assert_eq!(decimal_to_dms(-0.1275), "-0:07:39.00");
    }

    #[test]
    fn test_parse_coordinate() {
        assert!((parse_coordinate("51.5074").unwrap() - 51.5074).abs() < 0.000001);
        assert!((parse_coordinate("50:30:15").unwrap() - 50.504167).abs() < 0.000001);
        assert!((parse_coordinate(" -0.1278 ").unwrap() - -0.1278).abs() < 0.000001);
    }

    #[test]
    fn test_uk_coordinate_validation() {
        assert!(validate_uk_coordinates(51.5074, -0.1278).is_ok()); // London
        assert!(validate_uk_coordinates(55.9533, -3.1883).is_ok()); // Edinburgh
        assert!(validate_uk_coordinates(48.0, 0.0).is_err()); // Too far south
        assert!(validate_uk_coordinates(62.0, 0.0).is_err()); // Too far north
    }

    #[test]
    fn test_haversine_distance() {
        // London to Edinburgh
        let distance = haversine_distance(51.5074, -0.1278, 55.9533, -3.1883);
        assert!((distance - 534.0).abs() < 10.0); // ~534km with 10km tolerance
    }
}
