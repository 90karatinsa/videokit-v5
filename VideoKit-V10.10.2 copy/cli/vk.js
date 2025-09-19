#!/usr/bin/env node
/**
 * Minimal CLI dispatcher placeholder.
 */

function main() {
  const [,, command] = process.argv;
  switch (command) {
    case 'keygen':
    case 'stamp':
    case 'verify':
    case 'klv':
      console.error(`vk ${command} not yet implemented`);
      process.exitCode = 1;
      return;
    default:
      console.error('vk CLI bootstrap placeholder - no commands implemented');
      process.exitCode = 1;
  }
}

main();
