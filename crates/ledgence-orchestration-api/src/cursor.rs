//! Bounded lowercase hexadecimal representation shared by discovery cursors.
pub(crate) fn encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut cursor = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        cursor.push(char::from(HEX[usize::from(byte >> 4)]));
        cursor.push(char::from(HEX[usize::from(byte & 15)]));
    }
    cursor
}

pub(crate) fn decode(text: &str, maximum: usize) -> Option<Vec<u8>> {
    if text.is_empty() || text.len() > maximum || !text.len().is_multiple_of(2) {
        return None;
    }
    fn digit(byte: u8) -> Option<u8> {
        match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            _ => None,
        }
    }
    text.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| Some((digit(pair[0])? << 4) | digit(pair[1])?))
        .collect()
}
