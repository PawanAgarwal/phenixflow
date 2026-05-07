const fs = require('node:fs');
const path = require('node:path');

const projectRoot = path.resolve(__dirname, '..');
const codeRoots = ['src', 'scripts'];

function collectFiles(dir) {
  return fs.readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) return collectFiles(fullPath);
    return entry.isFile() && fullPath.endsWith('.js') ? [fullPath] : [];
  });
}

describe('PYM V5 Massive-only guardrails', () => {
  it('does not import forbidden data-source helpers', () => {
    const forbiddenProvider = 'click' + 'house';
    const importPattern = new RegExp(
      `(require\\([^)]*${forbiddenProvider}|from\\s+['"][^'"]*${forbiddenProvider}|import\\([^)]*${forbiddenProvider})`,
      'i',
    );
    const files = codeRoots.flatMap((root) => collectFiles(path.join(projectRoot, root)));
    expect(files.filter((file) => importPattern.test(fs.readFileSync(file, 'utf8')))).toEqual([]);
  });

  it('does not embed forbidden legacy option table references', () => {
    const schemaPattern = /['"`][^'"`]*options\.[^'"`]*['"`]/i;
    const files = codeRoots.flatMap((root) => collectFiles(path.join(projectRoot, root)));
    expect(files.filter((file) => schemaPattern.test(fs.readFileSync(file, 'utf8')))).toEqual([]);
  });
});
