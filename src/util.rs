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