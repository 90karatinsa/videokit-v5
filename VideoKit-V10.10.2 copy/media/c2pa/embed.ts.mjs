/**
 * Transport Stream PES embedding utilities (bootstrap placeholder).
 */

/**
 * Attempt to embed a C2PA manifest into an MPEG-TS container.
 * @param {Uint8Array} mediaBytes
 * @param {Uint8Array} manifestBytes
 * @returns {Promise<never>}
 */
export async function embedManifestInTransportStream(mediaBytes, manifestBytes) {
  void mediaBytes;
  void manifestBytes;
  throw new Error('TS C2PA embedding not yet implemented');
}

/**
 * Attempt to extract an embedded manifest from a TS container.
 * @param {Uint8Array} mediaBytes
 * @returns {Promise<never>}
 */
export async function extractManifestFromTransportStream(mediaBytes) {
  void mediaBytes;
  throw new Error('TS manifest extraction not yet implemented');
}
