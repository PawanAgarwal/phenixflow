const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');

const REPO_ROOT = path.resolve(__dirname, '..', '..', '..');
const TSLL_KERNEL_ID = 'tsll-seconds-passive-scalper.execution.v1';
const TSLL_KERNEL_ROOT = path.join(REPO_ROOT, 'packages', 'strategy-kernels', 'tsll-seconds-passive-scalper');
const TSLL_KERNEL_MANIFEST_URI = `/api/kernels/${TSLL_KERNEL_ID}/manifest`;
const TSLL_KERNEL_DOWNLOAD_URI = `/api/kernels/${TSLL_KERNEL_ID}/download`;
const TSLL_KERNEL_FILENAME = `${TSLL_KERNEL_ID}.zip`;
const ZIP_DOS_DATE_1980_01_01 = 33;

const REQUIRED_KERNEL_ARTIFACT_FILES = Object.freeze([
  'package.json',
  'kernel.manifest.json',
  'checksums.sha256.json',
  'index.js',
  'dist/kernel.mjs',
  'dist/features.mjs',
  'settings/default.json',
  'fixtures/replay-input.jsonl',
  'fixtures/expected-decisions.jsonl',
  'fixtures/expected-traces.jsonl',
  'scripts/replay-fixtures.js',
  'schemas/decision.schema.json',
  'schemas/event.schema.json',
  'schemas/trace.schema.json',
  'src/canonical.js',
  'src/features.js',
  'src/kernel.js',
]);

function canonicalize(value) {
  if (value === undefined) return undefined;
  if (value === null || typeof value !== 'object') return value;
  if (Array.isArray(value)) return value.map(canonicalize).filter((item) => item !== undefined);
  return Object.keys(value).sort().reduce((out, key) => {
    const next = canonicalize(value[key]);
    if (next !== undefined) out[key] = next;
    return out;
  }, {});
}

function sha256Canonical(value) {
  return crypto.createHash('sha256').update(JSON.stringify(canonicalize(value))).digest('hex');
}

function sha256Buffer(buffer) {
  return crypto.createHash('sha256').update(buffer).digest('hex');
}

function readKernelJson(fileName) {
  return JSON.parse(fs.readFileSync(path.join(TSLL_KERNEL_ROOT, fileName), 'utf8'));
}

function assertKernelId(kernelId) {
  if (kernelId !== TSLL_KERNEL_ID) {
    const error = new Error(`kernel_not_found:${kernelId}`);
    error.statusCode = 404;
    error.code = 'kernel_not_found';
    throw error;
  }
}

function safeKernelFilePath(relativePath) {
  if (typeof relativePath !== 'string' || relativePath.trim() === '') {
    throw new Error('invalid_kernel_artifact_file:empty_path');
  }
  if (path.isAbsolute(relativePath) || relativePath.split('/').includes('..')) {
    throw new Error(`invalid_kernel_artifact_file:unsafe_path:${relativePath}`);
  }
  return path.join(TSLL_KERNEL_ROOT, relativePath);
}

function kernelArtifactFiles(checksums) {
  const files = new Set([
    ...Object.keys(checksums.files || {}),
    ...REQUIRED_KERNEL_ARTIFACT_FILES,
  ]);
  const sortedFiles = [...files].sort();
  for (const relativePath of sortedFiles) {
    const filePath = safeKernelFilePath(relativePath);
    if (!fs.existsSync(filePath) || !fs.statSync(filePath).isFile()) {
      throw new Error(`missing_kernel_artifact_file:${relativePath}`);
    }
  }
  return sortedFiles;
}

const CRC32_TABLE = (() => {
  const table = new Uint32Array(256);
  for (let i = 0; i < 256; i += 1) {
    let c = i;
    for (let k = 0; k < 8; k += 1) {
      c = (c & 1) ? (0xedb88320 ^ (c >>> 1)) : (c >>> 1);
    }
    table[i] = c >>> 0;
  }
  return table;
})();

function crc32(buffer) {
  let crc = 0xffffffff;
  for (const byte of buffer) {
    crc = CRC32_TABLE[(crc ^ byte) & 0xff] ^ (crc >>> 8);
  }
  return (crc ^ 0xffffffff) >>> 0;
}

function localFileHeader(entry) {
  const header = Buffer.alloc(30);
  header.writeUInt32LE(0x04034b50, 0);
  header.writeUInt16LE(20, 4);
  header.writeUInt16LE(0, 6);
  header.writeUInt16LE(0, 8);
  header.writeUInt16LE(0, 10);
  header.writeUInt16LE(ZIP_DOS_DATE_1980_01_01, 12);
  header.writeUInt32LE(entry.crc, 14);
  header.writeUInt32LE(entry.data.length, 18);
  header.writeUInt32LE(entry.data.length, 22);
  header.writeUInt16LE(entry.nameBuffer.length, 26);
  header.writeUInt16LE(0, 28);
  return Buffer.concat([header, entry.nameBuffer, entry.data]);
}

function centralDirectoryHeader(entry) {
  const header = Buffer.alloc(46);
  header.writeUInt32LE(0x02014b50, 0);
  header.writeUInt16LE(20, 4);
  header.writeUInt16LE(20, 6);
  header.writeUInt16LE(0, 8);
  header.writeUInt16LE(0, 10);
  header.writeUInt16LE(0, 12);
  header.writeUInt16LE(ZIP_DOS_DATE_1980_01_01, 14);
  header.writeUInt32LE(entry.crc, 16);
  header.writeUInt32LE(entry.data.length, 20);
  header.writeUInt32LE(entry.data.length, 24);
  header.writeUInt16LE(entry.nameBuffer.length, 28);
  header.writeUInt16LE(0, 30);
  header.writeUInt16LE(0, 32);
  header.writeUInt16LE(0, 34);
  header.writeUInt16LE(0, 36);
  header.writeUInt32LE((0o100644 << 16) >>> 0, 38);
  header.writeUInt32LE(entry.offset, 42);
  return Buffer.concat([header, entry.nameBuffer]);
}

function endOfCentralDirectory(entryCount, centralDirectorySize, centralDirectoryOffset) {
  const header = Buffer.alloc(22);
  header.writeUInt32LE(0x06054b50, 0);
  header.writeUInt16LE(0, 4);
  header.writeUInt16LE(0, 6);
  header.writeUInt16LE(entryCount, 8);
  header.writeUInt16LE(entryCount, 10);
  header.writeUInt32LE(centralDirectorySize, 12);
  header.writeUInt32LE(centralDirectoryOffset, 16);
  header.writeUInt16LE(0, 20);
  return header;
}

function createZipBuffer(files) {
  const entries = files.map((relativePath) => {
    const data = fs.readFileSync(safeKernelFilePath(relativePath));
    return {
      name: relativePath,
      nameBuffer: Buffer.from(relativePath, 'utf8'),
      data,
      crc: crc32(data),
      offset: 0,
    };
  });
  const localParts = [];
  let offset = 0;
  for (const entry of entries) {
    entry.offset = offset;
    const part = localFileHeader(entry);
    localParts.push(part);
    offset += part.length;
  }
  const centralDirectoryOffset = offset;
  const centralParts = entries.map(centralDirectoryHeader);
  const centralDirectorySize = centralParts.reduce((sum, part) => sum + part.length, 0);
  return Buffer.concat([
    ...localParts,
    ...centralParts,
    endOfCentralDirectory(entries.length, centralDirectorySize, centralDirectoryOffset),
  ]);
}

function buildTsllKernelArtifact() {
  const manifest = readKernelJson('kernel.manifest.json');
  const checksums = readKernelJson('checksums.sha256.json');
  const files = kernelArtifactFiles(checksums);
  const buffer = createZipBuffer(files);
  const artifactSha256 = sha256Buffer(buffer);
  const checksumsSha256 = sha256Canonical(checksums);
  return {
    id: TSLL_KERNEL_ID,
    artifactUri: TSLL_KERNEL_MANIFEST_URI,
    downloadUri: TSLL_KERNEL_DOWNLOAD_URI,
    filename: TSLL_KERNEL_FILENAME,
    mediaType: 'application/zip',
    format: 'zip',
    artifactSha256,
    downloadSha256: artifactSha256,
    checksumsSha256,
    settingsSha256: manifest.settings.sha256,
    fixtureSuiteSha256: manifest.fixtures.suiteSha256,
    runtime: manifest.runtime,
    sidecarApi: manifest.runtime.sidecarApi,
    files,
    buffer,
  };
}

function getTsllKernelArtifactMetadata() {
  const { buffer: _buffer, ...metadata } = buildTsllKernelArtifact();
  return metadata;
}

function kernelManifestPayload(kernelId) {
  assertKernelId(kernelId);
  const manifest = readKernelJson('kernel.manifest.json');
  const checksums = readKernelJson('checksums.sha256.json');
  const artifact = getTsllKernelArtifactMetadata();
  return {
    manifest,
    checksums,
    artifact: {
      id: kernelId,
      artifactUri: artifact.artifactUri,
      downloadUri: artifact.downloadUri,
      filename: artifact.filename,
      mediaType: artifact.mediaType,
      format: artifact.format,
      sha256: artifact.artifactSha256,
      artifactSha256: artifact.artifactSha256,
      downloadSha256: artifact.downloadSha256,
      checksumsSha256: artifact.checksumsSha256,
      files: Object.keys(checksums.files || {}).sort(),
      packageFiles: artifact.files,
    },
  };
}

function kernelDownloadPayload(kernelId) {
  assertKernelId(kernelId);
  return buildTsllKernelArtifact();
}

module.exports = {
  TSLL_KERNEL_DOWNLOAD_URI,
  TSLL_KERNEL_ID,
  TSLL_KERNEL_MANIFEST_URI,
  TSLL_KERNEL_ROOT,
  getTsllKernelArtifactMetadata,
  kernelDownloadPayload,
  kernelManifestPayload,
};
