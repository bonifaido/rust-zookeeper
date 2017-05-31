#[macro_use]
extern crate zookeeper_derive;

use std::convert::From;
use std::error::Error;
use std::string::ToString;

#[derive(Debug, EnumConvertFromInt, EnumDisplay, EnumError, PartialEq)]
#[EnumConvertFromIntFallback = "C"]
enum BasicError {
    /// Documentation.
    A = 1,
    /// More documentation.
    B = 5,
    /// Yes.
    C = 10,
}

#[test]
fn display() {
    assert_eq!("A", BasicError::A.to_string());
    assert_eq!("B", BasicError::B.to_string());
    assert_eq!("C", BasicError::C.to_string());
}

#[test]
fn description() {
    assert_eq!("A", BasicError::A.description());
    assert_eq!("B", BasicError::B.description());
    assert_eq!("C", BasicError::C.description());
}

#[test]
fn from() {
    assert_eq!(BasicError::A, BasicError::from(1));
    assert_eq!(BasicError::B, BasicError::from(5));
    assert_eq!(BasicError::C, BasicError::from(10));
    // fallback to...
    assert_eq!(BasicError::C, BasicError::from(100));
}
