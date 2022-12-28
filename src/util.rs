pub fn read_le_u64(input: &mut &[u8]) -> u64 {
    let (int_bytes, rest) = input.split_at(std::mem::size_of::<u64>());
    *input = rest;
    u64::from_le_bytes(int_bytes.try_into().unwrap())
}

pub fn read_le_i64(input: &mut &[u8]) -> i64 {
    let (int_bytes, rest) = input.split_at(std::mem::size_of::<i64>());
    *input = rest;
    i64::from_le_bytes(int_bytes.try_into().unwrap())
}

pub fn convert_to_payload(key: String, value: u64) -> Vec<u8> {
    let key_bytes = key.into_bytes();
    let value_bytes = value.to_le_bytes();
    let key_size = key_bytes.len() as u64;
    let value_size = value_bytes.len() as u64;
    let mut results = vec![];
    results.append(key_size.to_le_bytes().to_vec().as_mut());
    results.append(value_size.to_le_bytes().to_vec().as_mut());
    results.append(key_bytes.to_vec().as_mut());
    results.append(value_bytes.to_vec().as_mut());

    results
}

pub fn read_payload(bytes: &[u8]) -> (String, u64) {
    let key_size = read_le_u64(&mut &bytes[0..8]);
    let value_size = read_le_u64(&mut &bytes[8..16]);

    let key_start = 16 as usize;
    let key_end = (16 + key_size) as usize;
    let value_start = (16 + key_size) as usize;
    let value_end = (16 + key_size + value_size) as usize;
    let key = &bytes[key_start..key_end];
    let mut value = &bytes[value_start..value_end];
    let key = String::from_utf8(key.to_vec()).unwrap();
    let value = read_le_u64(&mut value);
    return (key, value);
}

