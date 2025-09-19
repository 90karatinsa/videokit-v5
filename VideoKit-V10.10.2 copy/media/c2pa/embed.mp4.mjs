import { Buffer } from 'node:buffer';

export const JUMBF_BOX_UUID = Buffer.from([
  0x85, 0x4d, 0x44, 0xb6,
  0x8f, 0xd2, 0x4a, 0x3a,
  0x93, 0xcd, 0xc8, 0x0c,
  0x1e, 0xfb, 0x1d, 0x6e,
]);

function toBuffer(data) {
  if (Buffer.isBuffer(data)) {
    return data;
  }

  if (data instanceof Uint8Array) {
    return Buffer.from(data.buffer, data.byteOffset, data.byteLength);
  }

  if (typeof data === 'string') {
    return Buffer.from(data, 'utf8');
  }

  if (data == null) {
    return Buffer.alloc(0);
  }

  if (ArrayBuffer.isView(data)) {
    return Buffer.from(data.buffer, data.byteOffset, data.byteLength);
  }

  if (data instanceof ArrayBuffer) {
    return Buffer.from(data);
  }

  throw new TypeError('Unsupported data type for buffer conversion');
}

export function parseTopLevelBoxes(bufferLike) {
  const buffer = toBuffer(bufferLike);
  const boxes = [];
  let offset = 0;

  while (offset + 8 <= buffer.length) {
    let size = buffer.readUInt32BE(offset);
    const type = buffer.toString('ascii', offset + 4, offset + 8);
    let headerSize = 8;

    if (size === 1) {
      if (offset + 16 > buffer.length) {
        break;
      }

      size = Number(buffer.readBigUInt64BE(offset + 8));
      headerSize = 16;
    } else if (size === 0) {
      size = buffer.length - offset;
    }

    if (size < headerSize || offset + size > buffer.length) {
      break;
    }

    let payloadOffset = offset + headerSize;
    let uuid;

    if (type === 'uuid') {
      if (payloadOffset + 16 > buffer.length) {
        break;
      }

      uuid = buffer.slice(payloadOffset, payloadOffset + 16);
      payloadOffset += 16;
    }

    const payload = buffer.slice(payloadOffset, offset + size);

    boxes.push({
      type,
      size,
      start: offset,
      end: offset + size,
      headerSize,
      uuid: uuid ?? null,
      payloadOffset,
      payload,
    });

    if (size === 0) {
      break;
    }

    offset += size;
  }

  return boxes;
}

export function createUuidBox(payloadLike, uuidLike = JUMBF_BOX_UUID) {
  const payload = toBuffer(payloadLike);
  const uuid = toBuffer(uuidLike);

  if (uuid.length !== 16) {
    throw new TypeError('UUID boxes require a 16-byte identifier');
  }

  const size = 8 + uuid.length + payload.length;
  const header = Buffer.allocUnsafe(8 + uuid.length);

  header.writeUInt32BE(size, 0);
  header.write('uuid', 4, 4, 'ascii');
  uuid.copy(header, 8);

  return Buffer.concat([header, payload]);
}

export function findUuidBox(bufferLike, uuidLike = JUMBF_BOX_UUID) {
  const uuid = toBuffer(uuidLike);
  const boxes = parseTopLevelBoxes(bufferLike);

  for (const box of boxes) {
    if (box.type === 'uuid' && box.uuid && box.uuid.equals(uuid)) {
      return box;
    }
  }

  return null;
}

export function extractUuidPayload(bufferLike, uuidLike = JUMBF_BOX_UUID) {
  const box = findUuidBox(bufferLike, uuidLike);
  return box ? box.payload : null;
}

export { toBuffer };
