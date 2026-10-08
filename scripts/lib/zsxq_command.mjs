import { existsSync, readFileSync } from 'node:fs';
import { resolve } from 'node:path';

function isWindowsCommand(command) {
  return process.platform === 'win32' && /\.(?:cmd|bat)$/i.test(command);
}

export function getZsxqCommand(args) {
  if (process.env.ZSXQ_CLI) {
    return {
      command: process.env.ZSXQ_CLI,
      args,
      shell: isWindowsCommand(process.env.ZSXQ_CLI),
    };
  }

  const packagePath = resolve('node_modules/zsxq-cli/package.json');
  if (existsSync(packagePath)) {
    const pkg = JSON.parse(readFileSync(packagePath, 'utf8'));
    const bin = typeof pkg.bin === 'string'
      ? pkg.bin
      : pkg.bin?.['zsxq-cli'] || Object.values(pkg.bin || {})[0];
    if (bin) {
      return {
        command: process.execPath,
        args: [resolve('node_modules/zsxq-cli', bin), ...args],
        shell: false,
      };
    }
  }

  const globalCli = resolve(process.env.APPDATA || '', 'npm', 'zsxq-cli.cmd');
  return { command: globalCli, args, shell: true };
}
