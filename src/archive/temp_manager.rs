use crate::error::{ProcessingError, Result};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use tempfile::TempDir;
use zip::ZipArchive;

pub struct TempFileManager {
    temp_dir: TempDir,
    extracted_files: HashMap<String, PathBuf>,
}

impl TempFileManager {
    pub fn new() -> Result<Self> {
        let temp_dir = TempDir::new().map_err(|e| {
            ProcessingError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to create temporary directory: {}", e),
            ))
        })?;

        Ok(Self {
            temp_dir,
            extracted_files: HashMap::new(),
        })
    }

    pub fn temp_dir_path(&self) -> &Path {
        self.temp_dir.path()
    }

    pub fn extract_file(&mut self, zip_path: &Path, file_name: &str) -> Result<PathBuf> {
        // Check if already extracted
        if let Some(path) = self.extracted_files.get(file_name) {
            return Ok(path.clone());
        }

        let file = File::open(zip_path)?;
        let mut archive = ZipArchive::new(file)?;

        // Find the file in the archive
        let mut zip_file = archive.by_name(file_name).map_err(|_| {
            ProcessingError::InvalidFormat(format!(
                "File '{}' not found in archive '{}'",
                file_name,
                zip_path.display()
            ))
        })?;

        // Create destination path
        let dest_path = self.temp_dir.path().join(file_name);

        // Extract the file
        let mut dest_file = File::create(&dest_path)?;
        let mut writer = BufWriter::new(&mut dest_file);
        std::io::copy(&mut zip_file, &mut writer)?;
        writer.flush()?;

        // Track the extracted file
        self.extracted_files
            .insert(file_name.to_string(), dest_path.clone());

        Ok(dest_path)
    }

    pub fn extract_files_matching_pattern(
        &mut self,
        zip_path: &Path,
        pattern: &str,
    ) -> Result<Vec<PathBuf>> {
        let file = File::open(zip_path)?;
        let mut archive = ZipArchive::new(file)?;
        let mut extracted_paths = Vec::new();

        // Iterate through all files in the archive
        for i in 0..archive.len() {
            let mut zip_file = archive.by_index(i)?;
            let file_name = zip_file.name().to_string();

            // Check if file matches pattern
            if file_name.contains(pattern) {
                // Skip if already extracted
                if self.extracted_files.contains_key(&file_name) {
                    extracted_paths.push(self.extracted_files[&file_name].clone());
                    continue;
                }

                // Create destination path
                let dest_path = self.temp_dir.path().join(&file_name);

                // Create parent directories if needed
                if let Some(parent) = dest_path.parent() {
                    std::fs::create_dir_all(parent)?;
                }

                // Extract the file
                let mut dest_file = File::create(&dest_path)?;
                let mut writer = BufWriter::new(&mut dest_file);
                std::io::copy(&mut zip_file, &mut writer)?;
                writer.flush()?;

                // Track the extracted file
                self.extracted_files.insert(file_name, dest_path.clone());
                extracted_paths.push(dest_path);
            }
        }

        Ok(extracted_paths)
    }

    pub fn extract_metadata_files(&mut self, zip_path: &Path) -> Result<HashMap<String, PathBuf>> {
        let metadata_files = [
            "stations.txt",
            "elements.txt",
            "metadata.txt",
            "sources.txt",
        ];
        let mut extracted = HashMap::new();

        for file_name in &metadata_files {
            if let Ok(path) = self.extract_file(zip_path, file_name) {
                extracted.insert(file_name.to_string(), path);
            }
        }

        if extracted.is_empty() {
            return Err(ProcessingError::InvalidFormat(
                "No metadata files found in archive".to_string(),
            ));
        }

        Ok(extracted)
    }

    pub fn get_extracted_file(&self, file_name: &str) -> Option<&PathBuf> {
        self.extracted_files.get(file_name)
    }

    pub fn list_extracted_files(&self) -> Vec<&String> {
        self.extracted_files.keys().collect()
    }

    pub fn cleanup(&mut self) -> Result<()> {
        self.extracted_files.clear();

        // TempDir automatically cleans up on drop, so we don't need to call close() explicitly
        // The temporary directory will be cleaned up when TempDir is dropped
        Ok(())
    }

    pub fn estimate_extraction_size(&self, zip_path: &Path) -> Result<u64> {
        let file = File::open(zip_path)?;
        let mut archive = ZipArchive::new(file)?;
        let mut total_size = 0u64;

        for i in 0..archive.len() {
            let zip_file = archive.by_index(i)?;
            total_size += zip_file.size();
        }

        Ok(total_size)
    }
}

impl Drop for TempFileManager {
    fn drop(&mut self) {
        if let Err(e) = self.cleanup() {
            eprintln!("Warning: Failed to cleanup temporary files: {}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;
    use zip::{CompressionMethod, ZipWriter};

    fn create_test_zip() -> Result<NamedTempFile> {
        let file = NamedTempFile::new()?;
        {
            let mut zip = ZipWriter::new(&file);

            // Add stations.txt
            zip.start_file(
                "stations.txt",
                zip::write::FileOptions::default().compression_method(CompressionMethod::Stored),
            )?;
            zip.write_all(
                b"STAID,STANAME,CN,LAT,LON,HGHT\n257,TEST STATION,GB,+51:30:00,-000:07:00,100\n",
            )?;

            // Add elements.txt
            zip.start_file(
                "elements.txt",
                zip::write::FileOptions::default().compression_method(CompressionMethod::Stored),
            )?;
            zip.write_all(b"ELEID,DESC,UNIT\nTX1,Maximum temperature,0.1 C\n")?;

            // Add a data file
            zip.start_file(
                "TX_STAID000257.txt",
                zip::write::FileOptions::default().compression_method(CompressionMethod::Stored),
            )?;
            zip.write_all(b"Header\n101,20230101,125,0\n")?;

            zip.finish()?;
        } // zip goes out of scope here
        Ok(file)
    }

    #[test]
    fn test_temp_file_manager_creation() -> Result<()> {
        let manager = TempFileManager::new()?;
        assert!(manager.temp_dir_path().exists());
        Ok(())
    }

    #[test]
    fn test_extract_file() -> Result<()> {
        let test_zip = create_test_zip()?;
        let mut manager = TempFileManager::new()?;

        let extracted_path = manager.extract_file(test_zip.path(), "stations.txt")?;
        assert!(extracted_path.exists());

        let content = std::fs::read_to_string(&extracted_path)?;
        assert!(content.contains("TEST STATION"));

        Ok(())
    }

    #[test]
    fn test_extract_metadata_files() -> Result<()> {
        let test_zip = create_test_zip()?;
        let mut manager = TempFileManager::new()?;

        let metadata_files = manager.extract_metadata_files(test_zip.path())?;
        assert!(metadata_files.contains_key("stations.txt"));
        assert!(metadata_files.contains_key("elements.txt"));

        Ok(())
    }

    #[test]
    fn test_extract_files_matching_pattern() -> Result<()> {
        let test_zip = create_test_zip()?;
        let mut manager = TempFileManager::new()?;

        let data_files = manager.extract_files_matching_pattern(test_zip.path(), "STAID")?;
        assert_eq!(data_files.len(), 1);
        assert!(data_files[0]
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .contains("TX_STAID"));

        Ok(())
    }

    #[test]
    fn test_already_extracted_file() -> Result<()> {
        let test_zip = create_test_zip()?;
        let mut manager = TempFileManager::new()?;

        // Extract once
        let path1 = manager.extract_file(test_zip.path(), "stations.txt")?;
        // Extract again - should return same path
        let path2 = manager.extract_file(test_zip.path(), "stations.txt")?;

        assert_eq!(path1, path2);
        assert_eq!(manager.list_extracted_files().len(), 1);

        Ok(())
    }
}
