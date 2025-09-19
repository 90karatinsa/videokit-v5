import fs from 'node:fs/promises';
import path from 'node:path';
import {
  createPrivateKey,
  createPublicKey,
  createSign,
  createVerify,
  sign as signData,
  verify as verifyData,
} from 'node:crypto';
import {
  createUuidBox,
  extractUuidPayload,
  JUMBF_BOX_UUID,
  toBuffer,
} from '../media/c2pa/embed.mp4.mjs';

export function create() {
  throw new Error('create not implemented');
}

function normaliseBuffer(value) {
  return toBuffer(value ?? new Uint8Array());
}

function resolveAlgorithm(key, hint) {
  if (hint) {
    const lowered = hint.toLowerCase();
    if (lowered === 'ed25519' || lowered === 'p256' || lowered === 'es256') {
      return lowered === 'es256' ? 'p256' : lowered;
    }
  }

  const { asymmetricKeyType } = key;

  if (asymmetricKeyType === 'ed25519') {
    return 'ed25519';
  }

  if (asymmetricKeyType === 'ec') {
    return 'p256';
  }

  return null;
}

export async function signManifest(bytes, options = {}) {
  const manifest = normaliseBuffer(bytes);
  const { alg, pem } = options;

  if (!pem) {
    return {
      algorithm: 'none',
      signature: Buffer.alloc(0),
      manifest,
    };
  }

  const privateKey = createPrivateKey(pem);
  let algorithm = resolveAlgorithm(privateKey, alg);

  if (!algorithm) {
    throw new Error('Unsupported key type for signing');
  }

  let signature;

  if (algorithm === 'ed25519') {
    signature = signData(null, manifest, privateKey);
  } else if (algorithm === 'p256') {
    const signer = createSign('sha256');
    signer.update(manifest);
    signer.end();
    signature = signer.sign(privateKey);
  } else {
    throw new Error(`Unsupported signing algorithm: ${algorithm}`);
  }

  const publicKeyPem = createPublicKey(privateKey).export({
    type: 'spki',
    format: 'pem',
  });

  return {
    algorithm,
    signature,
    manifest,
    publicKey: publicKeyPem,
  };
}

function normaliseVerificationInput(input) {
  if (input && typeof input === 'object' && !Buffer.isBuffer(input) && !(input instanceof Uint8Array)) {
    const manifest = 'manifest' in input ? normaliseBuffer(input.manifest) : Buffer.alloc(0);
    const signature = 'signature' in input ? normaliseBuffer(input.signature) : Buffer.alloc(0);
    const algorithm = (input.algorithm ?? input.alg ?? 'none').toLowerCase();

    return {
      manifest,
      signature,
      algorithm,
      pem: input.pem,
      publicKey: input.publicKey,
    };
  }

  return {
    manifest: normaliseBuffer(input),
    signature: Buffer.alloc(0),
    algorithm: 'none',
    pem: undefined,
    publicKey: undefined,
  };
}

export async function verifyManifest(input) {
  const { manifest, signature, algorithm, pem, publicKey } = normaliseVerificationInput(input);

  if (!algorithm || algorithm === 'none') {
    return { valid: false, reason: 'unsigned' };
  }

  if (signature.length === 0) {
    return { valid: false, reason: 'missing signature' };
  }

  const keyMaterial = publicKey ?? pem;

  if (!keyMaterial) {
    return { valid: false, reason: 'missing public key' };
  }

  const key = createPublicKey(keyMaterial);
  let valid = false;

  if (algorithm === 'ed25519') {
    valid = verifyData(null, manifest, key, signature);
  } else if (algorithm === 'p256' || algorithm === 'es256') {
    const verifier = createVerify('sha256');
    verifier.update(manifest);
    verifier.end();
    valid = verifier.verify(key, signature);
  } else {
    return { valid: false, reason: `unsupported algorithm: ${algorithm}` };
  }

  return {
    valid,
    reason: valid ? null : 'signature mismatch',
  };
}

export async function embedInMp4(inputPath, manifestBytes, outPath, options = {}) {
  const manifest = normaliseBuffer(manifestBytes);
  const uuid = options.uuid ? normaliseBuffer(options.uuid) : JUMBF_BOX_UUID;
  const box = createUuidBox(manifest, uuid);
  const resolvedInput = path.resolve(inputPath);
  const resolvedOutput = path.resolve(outPath);

  await fs.mkdir(path.dirname(resolvedOutput), { recursive: true });

  if (resolvedInput !== resolvedOutput) {
    await fs.copyFile(resolvedInput, resolvedOutput);
  }

  await fs.appendFile(resolvedOutput, box);

  return {
    outputPath: resolvedOutput,
    bytesAppended: box.length,
    uuid,
  };
}

export async function extractManifest(inputPath, options = {}) {
  const uuid = options.uuid ? normaliseBuffer(options.uuid) : JUMBF_BOX_UUID;
  const data = await fs.readFile(path.resolve(inputPath));
  const payload = extractUuidPayload(data, uuid);
  return payload ?? null;
}

export async function embedInTs() {
  throw new Error('TS embed not implemented yet');
}
