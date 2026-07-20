// mod lease;
// mod pg;

use std::error::Error;

use salami::NodeId;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    Ok(())
}

// https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function#FNV_hash_parameters
#[allow(dead_code)]
fn node_id_of(name: &str) -> NodeId {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;
    let mut hash = FNV_OFFSET;
    for byte in name.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    NodeId::new(hash)
}
