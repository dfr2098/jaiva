//! Nivel Cold: segmentos append-only en disco local.
//!
//! Cada registro lleva cabecera con magic `JMC1`, payload LZ4 y SHA-256. El
//! índice en memoria se reconstruye al abrir leyendo los segmentos. Daños:
//! una cola cortada o con ceros se recorta; una cabecera dañada a mitad de
//! segmento se rescata dejando copia `.corrupt`; un checksum inválido al leer
//! cuenta como fallo de lectura, nunca como dato. Sin compactación todavía
//! (Paso 9): las versiones viejas ocupan espacio hasta la cuota.

use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{ErrorKind, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use lz4_flex::{compress, decompress};
use memmap2::{Mmap, MmapOptions};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::error::MemoryError;

const MAGIC: &[u8; 4] = b"JMC1";
const FLAG_VALUE: u8 = 1;
const FLAG_TOMBSTONE: u8 = 2;
const HEADER_LEN: usize = 4 + 1 + (4 * 4) + 32;
const MAX_FIELD_BYTES: usize = 16 * 1024 * 1024;
const MAX_PAYLOAD_BYTES: usize = 1024 * 1024 * 1024;

/// Objeto guardado en Cold.
#[derive(Debug, Clone, PartialEq)]
pub struct ColdEntry {
    pub class: String,
    pub value: Value,
}

/// Backend Cold. `put` debe rechazar (sin escribir) lo que exceda la cuota.
pub trait ColdStore: Send {
    fn get(&self, key: &str) -> Result<Option<ColdEntry>, MemoryError>;
    fn put(&mut self, key: &str, entry: ColdEntry) -> Result<(), MemoryError>;
    fn remove(&mut self, key: &str) -> Result<bool, MemoryError>;
    fn len(&self) -> usize;
    /// Bytes ocupados, incluidas versiones antiguas aún no compactadas.
    fn bytes_on_disk(&self) -> u64;
    fn max_disk_bytes(&self) -> Option<u64> {
        None
    }
    fn quota_rejections(&self) -> u64 {
        0
    }
    /// Segmentos rescatados al abrir por cabeceras dañadas.
    fn salvaged_segments(&self) -> u64 {
        0
    }
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn name(&self) -> &'static str;
}

/// Sin Cold (`cold.backend` ausente o `none`): no guarda nada.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopColdStore;

impl ColdStore for NoopColdStore {
    fn get(&self, _key: &str) -> Result<Option<ColdEntry>, MemoryError> {
        Ok(None)
    }

    fn put(&mut self, _key: &str, _entry: ColdEntry) -> Result<(), MemoryError> {
        Ok(())
    }

    fn remove(&mut self, _key: &str) -> Result<bool, MemoryError> {
        Ok(false)
    }

    fn len(&self) -> usize {
        0
    }

    fn bytes_on_disk(&self) -> u64 {
        0
    }

    fn name(&self) -> &'static str {
        "none"
    }
}

/// Cold en memoria para pruebas.
#[derive(Debug, Default)]
pub struct RecordingColdStore {
    pub entries: HashMap<String, ColdEntry>,
    pub puts: u64,
}

impl ColdStore for RecordingColdStore {
    fn get(&self, key: &str) -> Result<Option<ColdEntry>, MemoryError> {
        Ok(self.entries.get(key).cloned())
    }

    fn put(&mut self, key: &str, entry: ColdEntry) -> Result<(), MemoryError> {
        self.puts += 1;
        self.entries.insert(key.to_owned(), entry);
        Ok(())
    }

    fn remove(&mut self, key: &str) -> Result<bool, MemoryError> {
        Ok(self.entries.remove(key).is_some())
    }

    fn len(&self) -> usize {
        self.entries.len()
    }

    fn bytes_on_disk(&self) -> u64 {
        0
    }

    fn name(&self) -> &'static str {
        "recording"
    }
}

#[derive(Debug, Clone)]
struct RecordLocation {
    path: PathBuf,
    offset: u64,
    total_len: usize,
    class: String,
}

#[derive(Debug)]
struct SegmentWriter {
    id: u64,
    path: PathBuf,
    size: u64,
}

/// Cold local append-only: segmentos por clase, payload LZ4, checksum SHA-256
/// e índice reconstruible. Las lecturas usan mmap cuando está habilitado.
#[derive(Debug)]
pub struct SegmentedColdStore {
    root: PathBuf,
    segment_max_bytes: u64,
    mmap_reads: bool,
    index: HashMap<String, RecordLocation>,
    writers: HashMap<String, SegmentWriter>,
    maps: Mutex<HashMap<PathBuf, Arc<Mmap>>>,
    bytes_on_disk: u64,
    max_disk_bytes: Option<u64>,
    quota_rejections: u64,
    salvaged_segments: u64,
}

impl SegmentedColdStore {
    /// Abre sin cuota de disco.
    pub fn open(
        path: impl Into<PathBuf>,
        segment_max_bytes: u64,
        mmap_reads: bool,
    ) -> Result<Self, MemoryError> {
        Self::open_with_limit(path, segment_max_bytes, mmap_reads, None)
    }

    /// Abre `path`, reconstruye el índice (recortando o rescatando segmentos
    /// dañados) y aplica `max_disk_bytes` como cuota. Segmentos de al menos 4 KiB.
    pub fn open_with_limit(
        path: impl Into<PathBuf>,
        segment_max_bytes: u64,
        mmap_reads: bool,
        max_disk_bytes: Option<u64>,
    ) -> Result<Self, MemoryError> {
        let root = path.into();
        fs::create_dir_all(&root).map_err(|error| cold_io("crear directorio", &root, error))?;
        let rebuilt = rebuild_index(&root)?;
        Ok(Self {
            root,
            segment_max_bytes: segment_max_bytes.max(4096),
            mmap_reads,
            index: rebuilt.index,
            writers: HashMap::new(),
            maps: Mutex::new(HashMap::new()),
            bytes_on_disk: rebuilt.bytes_on_disk,
            max_disk_bytes,
            quota_rejections: 0,
            salvaged_segments: rebuilt.salvaged_segments,
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn append_record(
        &mut self,
        key: &str,
        class: &str,
        value: Option<&Value>,
    ) -> Result<RecordLocation, MemoryError> {
        let key_bytes = key.as_bytes();
        let class_bytes = class.as_bytes();
        validate_len("key", key_bytes.len(), MAX_FIELD_BYTES)?;
        validate_len("class", class_bytes.len(), MAX_FIELD_BYTES)?;

        let (flag, raw, payload, checksum) = match value {
            Some(value) => {
                let raw = serde_json::to_vec(value)
                    .map_err(|error| MemoryError::Cold(format!("serializar value: {error}")))?;
                validate_len("value", raw.len(), MAX_PAYLOAD_BYTES)?;
                let payload = compress(&raw);
                let checksum: [u8; 32] = Sha256::digest(&raw).into();
                (FLAG_VALUE, raw, payload, checksum)
            }
            None => (FLAG_TOMBSTONE, Vec::new(), Vec::new(), [0; 32]),
        };
        validate_len("payload comprimido", payload.len(), MAX_PAYLOAD_BYTES)?;

        let total_len = HEADER_LEN + key_bytes.len() + class_bytes.len() + payload.len();
        if value.is_some()
            && self
                .max_disk_bytes
                .is_some_and(|limit| self.bytes_on_disk.saturating_add(total_len as u64) > limit)
        {
            self.quota_rejections = self.quota_rejections.saturating_add(1);
            return Err(MemoryError::Cold(format!(
                "cuota de disco Cold agotada: uso={} bytes, registro={} bytes, límite={} bytes",
                self.bytes_on_disk,
                total_len,
                self.max_disk_bytes.expect("quota checked")
            )));
        }
        let segment_max_bytes = self.segment_max_bytes;
        let (path, offset) = {
            let writer = self.writer_for_class(class)?;
            if writer.size > 0 && writer.size.saturating_add(total_len as u64) > segment_max_bytes {
                writer.id += 1;
                writer.path =
                    segment_path(writer.path.parent().expect("segment parent"), writer.id);
                writer.size = 0;
            }
            (writer.path.clone(), writer.size)
        };

        if let Ok(mut maps) = self.maps.lock() {
            maps.remove(&path);
        }
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)
            .map_err(|error| cold_io("abrir segmento", &path, error))?;
        write_header(
            &mut file,
            flag,
            key_bytes.len(),
            class_bytes.len(),
            raw.len(),
            payload.len(),
            &checksum,
        )?;
        file.write_all(key_bytes)
            .and_then(|_| file.write_all(class_bytes))
            .and_then(|_| file.write_all(&payload))
            .map_err(|error| cold_io("escribir segmento", &path, error))?;
        file.sync_all()
            .map_err(|error| cold_io("fsync segmento", &path, error))?;
        self.writers.get_mut(class).expect("writer exists").size += total_len as u64;
        self.bytes_on_disk += total_len as u64;
        Ok(RecordLocation {
            path,
            offset,
            total_len,
            class: class.to_owned(),
        })
    }

    fn writer_for_class(&mut self, class: &str) -> Result<&mut SegmentWriter, MemoryError> {
        if !self.writers.contains_key(class) {
            let dir = self.root.join(class_directory(class));
            fs::create_dir_all(&dir).map_err(|error| cold_io("crear clase", &dir, error))?;
            let mut segments = segment_files(&dir)?;
            segments.sort();
            let (id, path, size) = match segments.last() {
                Some(path) => {
                    let id = segment_id(path).unwrap_or(0);
                    let size = fs::metadata(path)
                        .map_err(|error| cold_io("metadata segmento", path, error))?
                        .len();
                    (id, path.clone(), size)
                }
                None => (0, segment_path(&dir, 0), 0),
            };
            self.writers
                .insert(class.to_owned(), SegmentWriter { id, path, size });
        }
        Ok(self.writers.get_mut(class).expect("writer inserted"))
    }

    fn read_location(
        &self,
        expected_key: &str,
        location: &RecordLocation,
    ) -> Result<ColdEntry, MemoryError> {
        if self.mmap_reads {
            let mapped = self.map_segment(&location.path)?;
            let start = location.offset as usize;
            let end = start.saturating_add(location.total_len);
            if end <= mapped.len() {
                return decode_value_record(&mapped[start..end], expected_key);
            }
        }

        let mut file = File::open(&location.path)
            .map_err(|error| cold_io("abrir lectura", &location.path, error))?;
        file.seek(SeekFrom::Start(location.offset))
            .map_err(|error| cold_io("seek segmento", &location.path, error))?;
        let mut bytes = vec![0; location.total_len];
        file.read_exact(&mut bytes)
            .map_err(|error| cold_io("leer segmento", &location.path, error))?;
        decode_value_record(&bytes, expected_key)
    }

    fn map_segment(&self, path: &Path) -> Result<Arc<Mmap>, MemoryError> {
        let mut maps = self
            .maps
            .lock()
            .map_err(|_| MemoryError::Cold("mmap cache poisoned".to_owned()))?;
        if let Some(mapped) = maps.get(path) {
            return Ok(mapped.clone());
        }
        let file = File::open(path).map_err(|error| cold_io("abrir mmap", path, error))?;
        // SAFETY: los segmentos son append-only y nunca se truncan mientras el
        // store está abierto. Antes de append se invalida el mmap cacheado.
        let mapped = unsafe { MmapOptions::new().map(&file) }
            .map_err(|error| cold_io("crear mmap", path, error))?;
        let mapped = Arc::new(mapped);
        maps.insert(path.to_path_buf(), mapped.clone());
        Ok(mapped)
    }
}

impl ColdStore for SegmentedColdStore {
    fn get(&self, key: &str) -> Result<Option<ColdEntry>, MemoryError> {
        let Some(location) = self.index.get(key) else {
            return Ok(None);
        };
        self.read_location(key, location).map(Some)
    }

    fn put(&mut self, key: &str, entry: ColdEntry) -> Result<(), MemoryError> {
        if let Some(existing) = self.index.get(key)
            && existing.class != entry.class
        {
            return Err(MemoryError::Cold(format!(
                "la clave '{key}' ya pertenece a la clase '{}' y no puede moverse a '{}'",
                existing.class, entry.class
            )));
        }
        let location = self.append_record(key, &entry.class, Some(&entry.value))?;
        self.index.insert(key.to_owned(), location);
        Ok(())
    }

    fn remove(&mut self, key: &str) -> Result<bool, MemoryError> {
        let Some(existing) = self.index.get(key).cloned() else {
            return Ok(false);
        };
        self.append_record(key, &existing.class, None)?;
        self.index.remove(key);
        Ok(true)
    }

    fn len(&self) -> usize {
        self.index.len()
    }

    fn bytes_on_disk(&self) -> u64 {
        self.bytes_on_disk
    }

    fn max_disk_bytes(&self) -> Option<u64> {
        self.max_disk_bytes
    }

    fn quota_rejections(&self) -> u64 {
        self.quota_rejections
    }

    fn salvaged_segments(&self) -> u64 {
        self.salvaged_segments
    }

    fn name(&self) -> &'static str {
        "segmented_lz4"
    }
}

struct RebuiltIndex {
    index: HashMap<String, RecordLocation>,
    bytes_on_disk: u64,
    salvaged_segments: u64,
}

/// Damage found while indexing a segment. `Corrupt` is recoverable by
/// salvaging the segment; `Fatal` (I/O, inconsistent classes) is not.
enum ScanError {
    Corrupt(String),
    Fatal(MemoryError),
}

impl From<MemoryError> for ScanError {
    fn from(error: MemoryError) -> Self {
        Self::Fatal(error)
    }
}

fn rebuild_index(root: &Path) -> Result<RebuiltIndex, MemoryError> {
    let mut index = HashMap::new();
    let mut bytes_on_disk = 0u64;
    let mut salvaged_segments = 0u64;
    let mut class_dirs = fs::read_dir(root)
        .map_err(|error| cold_io("listar cold root", root, error))?
        .filter_map(Result::ok)
        .filter(|entry| entry.path().is_dir())
        .map(|entry| entry.path())
        .collect::<Vec<_>>();
    class_dirs.sort();
    for dir in class_dirs {
        let mut files = segment_files(&dir)?;
        files.sort();
        for path in files {
            let valid_len = match scan_segment(&path, &mut index) {
                Ok(valid_len) => valid_len,
                Err(ScanError::Corrupt(reason)) => {
                    salvage_segment(&path, &reason)?;
                    salvaged_segments += 1;
                    match scan_segment(&path, &mut index) {
                        Ok(valid_len) => valid_len,
                        Err(ScanError::Corrupt(reason)) => {
                            return Err(MemoryError::Cold(format!(
                                "segmento {} sigue dañado tras el rescate: {reason}",
                                path.display()
                            )));
                        }
                        Err(ScanError::Fatal(error)) => return Err(error),
                    }
                }
                Err(ScanError::Fatal(error)) => return Err(error),
            };
            let actual_len = fs::metadata(&path)
                .map_err(|error| cold_io("metadata segmento", &path, error))?
                .len();
            if valid_len < actual_len {
                OpenOptions::new()
                    .write(true)
                    .open(&path)
                    .and_then(|file| file.set_len(valid_len))
                    .map_err(|error| cold_io("recuperar cola parcial", &path, error))?;
            }
            bytes_on_disk += valid_len;
        }
    }
    Ok(RebuiltIndex {
        index,
        bytes_on_disk,
        salvaged_segments,
    })
}

fn scan_segment(
    path: &Path,
    index: &mut HashMap<String, RecordLocation>,
) -> Result<u64, ScanError> {
    let mut file = File::open(path).map_err(|error| cold_io("abrir índice", path, error))?;
    let file_len = file
        .metadata()
        .map_err(|error| cold_io("metadata índice", path, error))?
        .len();
    let mut offset = 0u64;
    while offset < file_len {
        let mut header_bytes = [0u8; HEADER_LEN];
        match file.read_exact(&mut header_bytes) {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::UnexpectedEof => {
                return tail_or_corrupt(path, offset);
            }
            Err(error) => return Err(cold_io("leer header", path, error).into()),
        }
        let Ok(header) = parse_header(&header_bytes) else {
            return tail_or_corrupt(path, offset);
        };
        let total_len = HEADER_LEN + header.key_len + header.class_len + header.payload_len;
        if offset.saturating_add(total_len as u64) > file_len {
            return tail_or_corrupt(path, offset);
        }
        let mut key = vec![0; header.key_len];
        let mut class = vec![0; header.class_len];
        file.read_exact(&mut key)
            .and_then(|_| file.read_exact(&mut class))
            .map_err(|error| cold_io("leer índice", path, error))?;
        file.seek(SeekFrom::Current(header.payload_len as i64))
            .map_err(|error| cold_io("saltar payload", path, error))?;
        let (Ok(key), Ok(class)) = (String::from_utf8(key), String::from_utf8(class)) else {
            return Err(ScanError::Corrupt(format!(
                "key/class UTF-8 inválida en offset {offset}"
            )));
        };
        match header.flag {
            FLAG_VALUE => {
                if let Some(existing) = index.get(&key)
                    && existing.class != class
                {
                    return Err(MemoryError::Cold(format!(
                        "clave '{key}' duplicada entre clases '{}' y '{class}'",
                        existing.class
                    ))
                    .into());
                }
                index.insert(
                    key,
                    RecordLocation {
                        path: path.to_path_buf(),
                        offset,
                        total_len,
                        class,
                    },
                );
            }
            FLAG_TOMBSTONE => {
                index.remove(&key);
            }
            other => {
                return Err(ScanError::Corrupt(format!(
                    "flag {other} inválido en offset {offset}"
                )));
            }
        }
        offset += total_len as u64;
    }
    Ok(offset)
}

/// Bytes after the last valid record are a torn tail (truncate) unless a
/// complete record follows them, which means the damage is in the middle.
fn tail_or_corrupt(path: &Path, offset: u64) -> Result<u64, ScanError> {
    let bytes = fs::read(path).map_err(|error| cold_io("leer cola", path, error))?;
    let rest = &bytes[offset as usize..];
    if find_record(rest, 1).is_some() {
        return Err(ScanError::Corrupt(format!(
            "registro ilegible en offset {offset} seguido de registros válidos"
        )));
    }
    Ok(offset)
}

/// Length of a structurally valid record starting at `bytes[0]`.
fn record_len(bytes: &[u8]) -> Option<usize> {
    let header = parse_header(bytes.get(..HEADER_LEN)?).ok()?;
    if header.flag != FLAG_VALUE && header.flag != FLAG_TOMBSTONE {
        return None;
    }
    let key_end = HEADER_LEN + header.key_len;
    let class_end = key_end + header.class_len;
    let total = class_end + header.payload_len;
    if total > bytes.len() {
        return None;
    }
    std::str::from_utf8(&bytes[HEADER_LEN..key_end]).ok()?;
    std::str::from_utf8(&bytes[key_end..class_end]).ok()?;
    Some(total)
}

fn find_record(bytes: &[u8], from: usize) -> Option<usize> {
    (from..bytes.len().saturating_sub(HEADER_LEN - 1))
        .find(|&start| bytes[start..].starts_with(MAGIC) && record_len(&bytes[start..]).is_some())
}

/// Keeps a byte-for-byte `.corrupt` copy, then rewrites the segment with the
/// records that are still structurally valid. Records inside the damaged range
/// are lost; an older version of those keys in an earlier segment becomes
/// visible again.
fn salvage_segment(path: &Path, reason: &str) -> Result<(), MemoryError> {
    let bytes = fs::read(path).map_err(|error| cold_io("leer para rescate", path, error))?;
    let mut kept = Vec::with_capacity(bytes.len());
    let mut offset = 0usize;
    let mut dropped = 0usize;
    while offset < bytes.len() {
        if let Some(total) = record_len(&bytes[offset..]) {
            kept.extend_from_slice(&bytes[offset..offset + total]);
            offset += total;
            continue;
        }
        let next = find_record(&bytes, offset + 1).unwrap_or(bytes.len());
        dropped += next - offset;
        offset = next;
    }

    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("segment");
    let mut quarantine = path.with_file_name(format!("{file_name}.corrupt"));
    let mut attempt = 1;
    while quarantine.exists() {
        quarantine = path.with_file_name(format!("{file_name}.corrupt.{attempt}"));
        attempt += 1;
    }
    fs::copy(path, &quarantine).map_err(|error| cold_io("copiar a cuarentena", path, error))?;

    let staging = path.with_file_name(format!("{file_name}.salvage"));
    let mut file =
        File::create(&staging).map_err(|error| cold_io("crear rescate", &staging, error))?;
    file.write_all(&kept)
        .and_then(|_| file.sync_all())
        .map_err(|error| cold_io("escribir rescate", &staging, error))?;
    fs::rename(&staging, path).map_err(|error| cold_io("publicar rescate", path, error))?;
    if let Some(parent) = path.parent()
        && let Ok(dir) = File::open(parent)
    {
        let _ = dir.sync_all();
    }
    tracing::warn!(
        segment = %path.display(),
        quarantine = %quarantine.display(),
        dropped_bytes = dropped,
        reason,
        "JME Cold segment salvaged"
    );
    Ok(())
}

#[derive(Debug)]
struct Header {
    flag: u8,
    key_len: usize,
    class_len: usize,
    raw_len: usize,
    payload_len: usize,
    checksum: [u8; 32],
}

fn write_header(
    file: &mut File,
    flag: u8,
    key_len: usize,
    class_len: usize,
    raw_len: usize,
    payload_len: usize,
    checksum: &[u8; 32],
) -> Result<(), MemoryError> {
    file.write_all(MAGIC)
        .and_then(|_| file.write_all(&[flag]))
        .and_then(|_| file.write_all(&(key_len as u32).to_le_bytes()))
        .and_then(|_| file.write_all(&(class_len as u32).to_le_bytes()))
        .and_then(|_| file.write_all(&(raw_len as u32).to_le_bytes()))
        .and_then(|_| file.write_all(&(payload_len as u32).to_le_bytes()))
        .and_then(|_| file.write_all(checksum))
        .map_err(|error| MemoryError::Cold(format!("escribir header: {error}")))
}

fn parse_header(bytes: &[u8]) -> Result<Header, MemoryError> {
    if bytes.len() < HEADER_LEN || &bytes[..4] != MAGIC {
        return Err(MemoryError::Cold("magic de segmento inválido".to_owned()));
    }
    let mut checksum = [0u8; 32];
    checksum.copy_from_slice(&bytes[21..53]);
    let header = Header {
        flag: bytes[4],
        key_len: u32::from_le_bytes(bytes[5..9].try_into().expect("slice")) as usize,
        class_len: u32::from_le_bytes(bytes[9..13].try_into().expect("slice")) as usize,
        raw_len: u32::from_le_bytes(bytes[13..17].try_into().expect("slice")) as usize,
        payload_len: u32::from_le_bytes(bytes[17..21].try_into().expect("slice")) as usize,
        checksum,
    };
    validate_header(&header)?;
    Ok(header)
}

fn validate_header(header: &Header) -> Result<(), MemoryError> {
    validate_len("key", header.key_len, MAX_FIELD_BYTES)?;
    validate_len("class", header.class_len, MAX_FIELD_BYTES)?;
    validate_len("raw", header.raw_len, MAX_PAYLOAD_BYTES)?;
    validate_len("payload", header.payload_len, MAX_PAYLOAD_BYTES)?;
    Ok(())
}

fn decode_value_record(bytes: &[u8], expected_key: &str) -> Result<ColdEntry, MemoryError> {
    let header = parse_header(bytes)?;
    if header.flag != FLAG_VALUE {
        return Err(MemoryError::Cold("el índice apunta a tombstone".to_owned()));
    }
    let key_start = HEADER_LEN;
    let class_start = key_start + header.key_len;
    let payload_start = class_start + header.class_len;
    let payload_end = payload_start + header.payload_len;
    if payload_end > bytes.len() {
        return Err(MemoryError::Cold("registro cold truncado".to_owned()));
    }
    let key = std::str::from_utf8(&bytes[key_start..class_start])
        .map_err(|error| MemoryError::Cold(format!("key UTF-8 inválida: {error}")))?;
    if key != expected_key {
        return Err(MemoryError::Cold(format!(
            "índice inconsistente: esperado '{expected_key}', encontrado '{key}'"
        )));
    }
    let class = std::str::from_utf8(&bytes[class_start..payload_start])
        .map_err(|error| MemoryError::Cold(format!("class UTF-8 inválida: {error}")))?
        .to_owned();
    let raw = decompress(&bytes[payload_start..payload_end], header.raw_len)
        .map_err(|error| MemoryError::Cold(format!("descomprimir LZ4: {error}")))?;
    let checksum: [u8; 32] = Sha256::digest(&raw).into();
    if checksum != header.checksum {
        return Err(MemoryError::Cold(format!(
            "checksum inválido para '{expected_key}'"
        )));
    }
    let value = serde_json::from_slice(&raw)
        .map_err(|error| MemoryError::Cold(format!("JSON cold inválido: {error}")))?;
    Ok(ColdEntry { class, value })
}

fn class_directory(class: &str) -> String {
    let digest = Sha256::digest(class.as_bytes());
    let short: String = digest[..12]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    format!("class-{short}")
}

fn segment_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("segment-{id:020}.jmc"))
}

fn segment_files(dir: &Path) -> Result<Vec<PathBuf>, MemoryError> {
    Ok(fs::read_dir(dir)
        .map_err(|error| cold_io("listar segmentos", dir, error))?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|value| value.to_str()) == Some("jmc"))
        .collect())
}

fn segment_id(path: &Path) -> Option<u64> {
    path.file_stem()?
        .to_str()?
        .strip_prefix("segment-")?
        .parse()
        .ok()
}

fn validate_len(label: &str, value: usize, maximum: usize) -> Result<(), MemoryError> {
    if value > maximum || value > u32::MAX as usize {
        return Err(MemoryError::Cold(format!(
            "{label} demasiado grande: {value} bytes (máximo {maximum})"
        )));
    }
    Ok(())
}

fn cold_io(action: &str, path: &Path, error: std::io::Error) -> MemoryError {
    MemoryError::Cold(format!("{action} {}: {error}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn segmented_round_trip_remove_and_reopen() {
        let dir = scratch("roundtrip");
        {
            let mut store = SegmentedColdStore::open(&dir, 4096, true).unwrap();
            store
                .put(
                    "carrier:A12",
                    ColdEntry {
                        class: "carrier".to_owned(),
                        value: json!({"lane": 3, "payload": "x".repeat(2048)}),
                    },
                )
                .unwrap();
            assert_eq!(store.len(), 1);
            assert_eq!(store.get("carrier:A12").unwrap().unwrap().value["lane"], 3);
        }
        {
            let mut reopened = SegmentedColdStore::open(&dir, 4096, true).unwrap();
            assert_eq!(
                reopened.get("carrier:A12").unwrap().unwrap().value["lane"],
                3
            );
            assert!(reopened.remove("carrier:A12").unwrap());
        }
        let reopened = SegmentedColdStore::open(&dir, 4096, true).unwrap();
        assert!(reopened.get("carrier:A12").unwrap().is_none());
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn rotates_small_segments_and_recovers_partial_tail() {
        let dir = scratch("rotate");
        let mut store = SegmentedColdStore::open(&dir, 4096, false).unwrap();
        for id in 0..12 {
            let mut state = id as u64 + 1;
            let payload = (0..1024)
                .map(|_| {
                    state = state
                        .wrapping_mul(6_364_136_223_846_793_005)
                        .wrapping_add(1);
                    state
                })
                .collect::<Vec<_>>();
            store
                .put(
                    &format!("telemetry:{id}"),
                    ColdEntry {
                        class: "telemetry".to_owned(),
                        value: json!({"id": id, "payload": payload}),
                    },
                )
                .unwrap();
        }
        assert_eq!(store.len(), 12);
        let segment_count = fs::read_dir(store.root().join(class_directory("telemetry")))
            .unwrap()
            .filter_map(Result::ok)
            .count();
        assert!(segment_count >= 2);
        drop(store);

        let mut files = segment_files(&dir.join(class_directory("telemetry"))).unwrap();
        files.sort();
        let last = files.last().unwrap();
        let clean_len = fs::metadata(last).unwrap().len();
        OpenOptions::new()
            .append(true)
            .open(last)
            .unwrap()
            .write_all(b"partial")
            .unwrap();
        let reopened = SegmentedColdStore::open(&dir, 4096, true).unwrap();
        assert_eq!(reopened.len(), 12);
        assert_eq!(fs::metadata(last).unwrap().len(), clean_len);
        assert_eq!(
            reopened.get("telemetry:11").unwrap().unwrap().value["id"],
            11
        );
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn disk_quota_rejects_value_without_publishing_it() {
        let dir = scratch("quota");
        let mut store = SegmentedColdStore::open_with_limit(&dir, 4096, false, Some(64)).unwrap();
        let error = store
            .put(
                "carrier:A12",
                ColdEntry {
                    class: "carrier".to_owned(),
                    value: json!({"lane": 3}),
                },
            )
            .unwrap_err();
        assert!(error.to_string().contains("cuota de disco Cold"));
        assert_eq!(store.len(), 0);
        assert_eq!(store.bytes_on_disk(), 0);
        assert_eq!(store.quota_rejections(), 1);
        let _ = fs::remove_dir_all(dir);
    }

    fn filled_segment(label: &str, records: usize) -> (PathBuf, PathBuf) {
        let dir = scratch(label);
        let mut store = SegmentedColdStore::open(&dir, 1 << 20, false).unwrap();
        for id in 0..records {
            store
                .put(
                    &format!("carrier:{id}"),
                    ColdEntry {
                        class: "carrier".to_owned(),
                        value: json!({"id": id, "payload": format!("{id:x}").repeat(64)}),
                    },
                )
                .unwrap();
        }
        drop(store);
        let segment = segment_files(&dir.join(class_directory("carrier")))
            .unwrap()
            .pop()
            .unwrap();
        (dir, segment)
    }

    fn record_offsets(bytes: &[u8]) -> Vec<usize> {
        let mut offsets = Vec::new();
        let mut offset = 0;
        while let Some(total) = record_len(&bytes[offset..]) {
            offsets.push(offset);
            offset += total;
        }
        offsets
    }

    #[test]
    fn zero_filled_tail_is_truncated_like_a_torn_write() {
        let (dir, segment) = filled_segment("zeros", 3);
        let clean_len = fs::metadata(&segment).unwrap().len();
        OpenOptions::new()
            .append(true)
            .open(&segment)
            .unwrap()
            .write_all(&[0; 4096])
            .unwrap();
        let store = SegmentedColdStore::open(&dir, 1 << 20, false).unwrap();
        assert_eq!(store.len(), 3);
        assert_eq!(store.salvaged_segments(), 0);
        assert_eq!(fs::metadata(&segment).unwrap().len(), clean_len);
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn corrupt_header_mid_segment_is_salvaged_and_quarantined() {
        let (dir, segment) = filled_segment("salvage", 4);
        let original = fs::read(&segment).unwrap();
        let offsets = record_offsets(&original);
        let mut damaged = original.clone();
        damaged[offsets[1]] ^= 0xFF;
        fs::write(&segment, &damaged).unwrap();

        let store = SegmentedColdStore::open(&dir, 1 << 20, true).unwrap();
        assert_eq!(store.salvaged_segments(), 1);
        assert_eq!(store.len(), 3);
        assert!(store.get("carrier:1").unwrap().is_none());
        for id in [0, 2, 3] {
            assert_eq!(
                store.get(&format!("carrier:{id}")).unwrap().unwrap().value["id"],
                id
            );
        }
        let quarantine = segment.with_file_name(format!(
            "{}.corrupt",
            segment.file_name().unwrap().to_str().unwrap()
        ));
        assert_eq!(fs::read(&quarantine).unwrap(), damaged);
        drop(store);

        let reopened = SegmentedColdStore::open(&dir, 1 << 20, false).unwrap();
        assert_eq!(reopened.salvaged_segments(), 0);
        assert_eq!(reopened.len(), 3);
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn corrupt_length_mid_segment_does_not_truncate_later_records() {
        let (dir, segment) = filled_segment("length", 4);
        let mut bytes = fs::read(&segment).unwrap();
        let offsets = record_offsets(&bytes);
        bytes[offsets[1] + 17..offsets[1] + 21].copy_from_slice(&(1u32 << 20).to_le_bytes());
        fs::write(&segment, &bytes).unwrap();

        let store = SegmentedColdStore::open(&dir, 1 << 20, false).unwrap();
        assert_eq!(store.salvaged_segments(), 1);
        for id in [0, 2, 3] {
            assert!(
                store.get(&format!("carrier:{id}")).unwrap().is_some(),
                "{id}"
            );
        }
        let _ = fs::remove_dir_all(dir);
    }

    fn scratch(label: &str) -> PathBuf {
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/jme-test")
            .join(format!(
                "cold-{label}-{}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
        fs::create_dir_all(&dir).unwrap();
        dir
    }
}
