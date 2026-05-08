use std::collections::HashMap;

pub struct Redisdb {
    db: HashMap<String, Vec<u8>>,
}

impl Redisdb {
    pub fn insert(&mut self, uid: &str, data: Vec<u8>) -> bool {
        match self.db.insert(uid.into(), data) {
            Some(v) => { true }
            None => { false }
        }
    }

    pub fn get<'a>(&self, uid: &'a str) -> Option<&Vec<u8>> {
        self.db.get(uid)
    }

    pub fn new() -> Redisdb {
        Redisdb { db: HashMap::new() }
    }
}
