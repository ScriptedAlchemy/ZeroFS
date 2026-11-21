use super::*;

#[test]
fn serial_parser_accepts_one_trimmed_value_and_rejects_unavailable_evidence() {
    assert_eq!(
        parse_serial("EXAMPLE-NVME-0001\n").unwrap(),
        Serial::try_from("EXAMPLE-NVME-0001".to_owned()).unwrap()
    );
    for invalid in [
        "",
        " \n",
        "EXAMPLE-ONE\nEXAMPLE-TWO",
        "EXAMPLE-ONE\tEXAMPLE-TWO",
    ] {
        assert!(parse_serial(invalid).is_err());
    }
}
