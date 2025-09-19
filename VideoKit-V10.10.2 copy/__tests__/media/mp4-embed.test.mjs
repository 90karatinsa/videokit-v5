import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { embedInMp4, extractManifest } from '../../shims/contentauth.mjs';

function box(type, payload) {
  const payloadBuffer = Buffer.isBuffer(payload) ? payload : Buffer.from(payload);
  const size = 8 + payloadBuffer.length;
  const buffer = Buffer.alloc(size);
  buffer.writeUInt32BE(size, 0);
  buffer.write(type, 4, 4, 'ascii');
  payloadBuffer.copy(buffer, 8);
  return buffer;
}

function createMinimalMp4() {
  const ftypPayload = Buffer.concat([
    Buffer.from('isom', 'ascii'),
    Buffer.from([0x00, 0x00, 0x02, 0x00]),
    Buffer.from('isom', 'ascii'),
    Buffer.from('iso2', 'ascii'),
  ]);

  const freePayload = Buffer.alloc(4, 0);
  const mdatPayload = Buffer.from([0x00]);

  return Buffer.concat([
    box('ftyp', ftypPayload),
    box('free', freePayload),
    box('mdat', mdatPayload),
  ]);
}

test('embedInMp4 appends and extractManifest recovers the payload', async () => {
  const workDir = await mkdtemp(path.join(tmpdir(), 'vk-mp4-'));
  const inputPath = path.join(workDir, 'input.mp4');
  const outputPath = path.join(workDir, 'output.mp4');
  const mp4Bytes = createMinimalMp4();
  const manifest = Buffer.from('{"test":"payload"}', 'utf8');

  await writeFile(inputPath, mp4Bytes);

  await embedInMp4(inputPath, manifest, outputPath);
  const extracted = await extractManifest(outputPath);

  assert.ok(extracted, 'manifest should be extractable');
  assert.equal(Buffer.compare(extracted, manifest), 0, 'manifest payload should match');
});
