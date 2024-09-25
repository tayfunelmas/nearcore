use near_primitives_core::{hash::hash as sha256, types::CodeHash};

pub struct ContractCode {
    code: Vec<u8>,
    hash: CodeHash,
}

impl ContractCode {
    pub fn new(code: Vec<u8>, hash: Option<CodeHash>) -> ContractCode {
        let hash = hash.unwrap_or_else(|| sha256(&code));
        debug_assert_eq!(hash, sha256(&code));

        ContractCode { code, hash }
    }

    pub fn code(&self) -> &[u8] {
        self.code.as_slice()
    }

    pub fn into_code(self) -> Vec<u8> {
        self.code
    }

    pub fn hash(&self) -> &CodeHash {
        &self.hash
    }

    pub fn clone_for_tests(&self) -> Self {
        Self { code: self.code.clone(), hash: self.hash }
    }
}
