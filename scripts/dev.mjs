import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const viteBin = resolve(root, 'node_modules/vite/bin/vite.js');
const passed = process.argv.slice(2).filter((argument) => argument !== '--');
const hasHost = passed.some((argument) => argument === '--host' || argument.startsWith('--host='));
const hasPort = passed.some((argument) => argument === '--port' || argument.startsWith('--port='));
const args = [
  viteBin,
  ...(hasHost ? [] : ['--host', '127.0.0.1']),
  ...(hasPort ? [] : ['--port', '5219']),
  ...passed
];

const child = spawn(process.execPath, args, { cwd: root, stdio: 'inherit' });
child.on('exit', (code, signal) => {
  if (signal) {
    process.kill(process.pid, signal);
  } else {
    process.exit(code ?? 0);
  }
});
