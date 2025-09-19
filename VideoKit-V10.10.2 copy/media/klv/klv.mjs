/**
 * Bootstrap placeholder for MISB KLV encode/decode utilities.
 */

/**
 * Convert MISB JSON into a binary KLV payload.
 * @param {object} json
 * @returns {Promise<Uint8Array>}
 */
export async function jsonToKlv(json) {
  void json;
  throw new Error('MISB JSON→KLV not yet implemented');
}

/**
 * Convert a binary KLV payload into MISB JSON.
 * @param {Uint8Array} klv
 * @returns {Promise<object>}
 */
export async function klvToJson(klv) {
  void klv;
  throw new Error('MISB KLV→JSON not yet implemented');
}
