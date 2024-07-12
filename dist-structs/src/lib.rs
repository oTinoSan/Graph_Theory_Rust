use lamellar::{Serialize, Deserialize};

#[derive(Copy, Debug, Clone, Serialize, Deserialize, lamellar::ArrayOps, Default, PartialEq)]
pub struct Edge (pub u64, pub u64);

impl lamellar::memregion::Dist for Edge {}

impl From<(u64, u64)> for Edge {
    fn from(e: (u64, u64)) -> Self {
        Self(e.0, e.1)
    }
}