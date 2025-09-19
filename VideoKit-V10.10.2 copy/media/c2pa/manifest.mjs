/**
 * Placeholder manifest helpers for the C2PA media kit bootstrap.
 */

/**
 * Build a manifest payload from high level inputs.
 * @param {object} manifest
 * @returns {Promise<Uint8Array>}
 */
export async function createManifest(manifest = {}) {
  void manifest;
  throw new Error('Manifest creation not yet implemented');
}

/**
 * Parse a manifest payload into JSON.
 * @param {Uint8Array} payload
 * @returns {Promise<object>}
 */
export async function parseManifest(payload) {
  void payload;
  throw new Error('Manifest parsing not yet implemented');
}
