### Pack A – Bootstrap

- **Repo tree additions**
  - `cli/vk.js`
  - `fixtures/media/.keep`
  - `media/c2pa/embed.mp4.mjs`
  - `media/c2pa/embed.ts.mjs`
  - `media/c2pa/manifest.mjs`
  - `media/klv/klv.mjs`
  - `media/klv/schema.json`
  - `shims/contentauth.mjs`
- **Runtime check**
  - Existing runtime untouched; no new imports were added to `server.mjs` or middleware.
  - `npm test` / `npm start` not available in current snapshot (no package metadata); nothing to execute yet.
- **NEXT (Packs B–F)**
  - Pack B – Contentauth adapter wiring.
  - Pack C – PEM key management + signer updates.
  - Pack D – Media embedding helpers + CLI/server integration.
  - Pack E – MISB schema/processor expansion.
  - Pack F – Post-hoc policy, testing, and reporting.

### Pack B – Shim + MP4 Embed

- **Code excerpts**
  - `shims/contentauth.mjs`
    ```diff
    +import fs from 'node:fs/promises';
    +import path from 'node:path';
    +import {
    +  createPrivateKey,
    +  createPublicKey,
    +  createSign,
    +  createVerify,
    +  sign as signData,
    +  verify as verifyData,
    +} from 'node:crypto';
    +
    +export async function embedInMp4(inputPath, manifestBytes, outPath, options = {}) {
    +  const manifest = normaliseBuffer(manifestBytes);
    +  const uuid = options.uuid ? normaliseBuffer(options.uuid) : JUMBF_BOX_UUID;
    +  const box = createUuidBox(manifest, uuid);
    +  const resolvedInput = path.resolve(inputPath);
    +  const resolvedOutput = path.resolve(outPath);
    +
    +  await fs.mkdir(path.dirname(resolvedOutput), { recursive: true });
    +
    +  if (resolvedInput !== resolvedOutput) {
    +    await fs.copyFile(resolvedInput, resolvedOutput);
    +  }
    +
    +  await fs.appendFile(resolvedOutput, box);
    +
    +  return {
    +    outputPath: resolvedOutput,
    +    bytesAppended: box.length,
    +    uuid,
    +  };
    +}
    ```
  - `media/c2pa/embed.mp4.mjs`
    ```diff
    +import { Buffer } from 'node:buffer';
    +
    +export const JUMBF_BOX_UUID = Buffer.from([
    +  0x85, 0x4d, 0x44, 0xb6,
    +  0x8f, 0xd2, 0x4a, 0x3a,
    +  0x93, 0xcd, 0xc8, 0x0c,
    +  0x1e, 0xfb, 0x1d, 0x6e,
    +]);
    +
    +export function parseTopLevelBoxes(bufferLike) {
    +  const buffer = toBuffer(bufferLike);
    +  const boxes = [];
    +  let offset = 0;
    +
    +  while (offset + 8 <= buffer.length) {
    +    let size = buffer.readUInt32BE(offset);
    +    const type = buffer.toString('ascii', offset + 4, offset + 8);
    +    let headerSize = 8;
    +
    +    if (size === 1) {
    +      if (offset + 16 > buffer.length) {
    +        break;
    +      }
    +
    +      size = Number(buffer.readBigUInt64BE(offset + 8));
    +      headerSize = 16;
    +    } else if (size === 0) {
    +      size = buffer.length - offset;
    +    }
    +
    +    if (size < headerSize || offset + size > buffer.length) {
    +      break;
    +    }
    +
    +    let payloadOffset = offset + headerSize;
    +    let uuid;
    +
    +    if (type === 'uuid') {
    +      if (payloadOffset + 16 > buffer.length) {
    +        break;
    +      }
    +
    +      uuid = buffer.slice(payloadOffset, payloadOffset + 16);
    +      payloadOffset += 16;
    +    }
    +
    +    const payload = buffer.slice(payloadOffset, offset + size);
    +
    +    boxes.push({
    +      type,
    +      size,
    +      start: offset,
    +      end: offset + size,
    +      headerSize,
    +      uuid: uuid ?? null,
    +      payloadOffset,
    +      payload,
    +    });
    +
    +    if (size === 0) {
    +      break;
    +    }
    +
    +    offset += size;
    +  }
    +
    +  return boxes;
    +}
    ```
- **Tests**
  - `node --test __tests__/media/mp4-embed.test.mjs`
    ```
    ✔ embedInMp4 appends and extractManifest recovers the payload (13.727009ms)
    ℹ tests 1
    ℹ pass 1
    ```
- **NEXT**
  - Wire CLI and core flows to use the shim helpers once upstream contracts are ready.
