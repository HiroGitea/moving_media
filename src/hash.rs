use anyhow::Result;
use std::path::Path;

/// 用 BLAKE3 计算文件哈希，返回 64 位小写十六进制字符串。
///
/// BLAKE3 自动利用 AVX2 / AVX-512 / NEON SIMD 指令；
/// 文件 ≥ 128 KB 时启用 Rayon 多线程并行哈希，充分发挥多核性能。
pub fn hash_file(path: &Path) -> Result<String> {
    // memory-map 大文件以减少系统调用，并允许 blake3 在线程间并行读取
    let file = std::fs::File::open(path)?;
    let meta = file.metadata()?;

    if meta.len() >= 128 * 1024 {
        // 大文件：mmap + Rayon 多线程并行哈希
        let mmap = unsafe { memmap2::Mmap::map(&file)? };
        let hash = blake3::Hasher::new().update_rayon(&mmap).finalize();
        Ok(hash.to_hex().to_string())
    } else {
        // 小文件：直接流式读取
        use std::io::Read;
        let mut hasher = blake3::Hasher::new();
        let mut buf = vec![0u8; 64 * 1024];
        let mut reader = std::io::BufReader::new(file);
        loop {
            let n = reader.read(&mut buf)?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
        }
        Ok(hasher.finalize().to_hex().to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_hash_length_and_deterministic() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bin");
        std::fs::File::create(&path)
            .unwrap()
            .write_all(b"hello world")
            .unwrap();
        let h1 = hash_file(&path).unwrap();
        let h2 = hash_file(&path).unwrap();
        assert_eq!(h1.len(), 64, "BLAKE3 hex should be 64 chars");
        assert_eq!(h1, h2, "same file should produce same hash");
    }

    #[test]
    fn test_hash_different_contents() {
        let dir = tempfile::tempdir().unwrap();
        let p1 = dir.path().join("a.bin");
        let p2 = dir.path().join("b.bin");
        std::fs::File::create(&p1)
            .unwrap()
            .write_all(b"aaa")
            .unwrap();
        std::fs::File::create(&p2)
            .unwrap()
            .write_all(b"bbb")
            .unwrap();
        assert_ne!(hash_file(&p1).unwrap(), hash_file(&p2).unwrap());
    }

    #[test]
    fn test_hash_large_file_parallel() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("large.bin");
        // 写入 256 KB，触发 mmap + rayon 路径
        let data = vec![0xABu8; 256 * 1024];
        std::fs::File::create(&path)
            .unwrap()
            .write_all(&data)
            .unwrap();
        let h1 = hash_file(&path).unwrap();
        let h2 = hash_file(&path).unwrap();
        assert_eq!(h1.len(), 64);
        assert_eq!(h1, h2);
    }
}
