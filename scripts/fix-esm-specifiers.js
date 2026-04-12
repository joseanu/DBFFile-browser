const fs = require('fs');
const path = require('path');

const DIST_DIR = path.join(__dirname, '..', 'dist');

function shouldRewriteSpecifier(specifier) {
  if (!specifier.startsWith('./')) return false;
  if (specifier.endsWith('.js') || specifier.endsWith('.mjs') || specifier.endsWith('.cjs')) {
    return false;
  }
  if (specifier.endsWith('.json')) return false;
  return true;
}

function rewriteImports(source) {
  return source.replace(/(\b(?:import|export)\b[\s\S]*?\bfrom\s*['"])(\.\/[^'"\n]+)(['"])/g, (full, prefix, specifier, suffix) => {
    if (!shouldRewriteSpecifier(specifier)) return full;
    return `${prefix}${specifier}.js${suffix}`;
  });
}

function walkJsFiles(dir) {
  const entries = fs.readdirSync(dir, { withFileTypes: true });
  const files = [];
  for (const entry of entries) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      files.push(...walkJsFiles(fullPath));
      continue;
    }
    if (entry.isFile() && entry.name.endsWith('.js')) {
      files.push(fullPath);
    }
  }
  return files;
}

if (!fs.existsSync(DIST_DIR)) {
  process.exit(0);
}

const files = walkJsFiles(DIST_DIR);
for (const file of files) {
  const original = fs.readFileSync(file, 'utf8');
  const updated = rewriteImports(original);
  if (updated !== original) {
    fs.writeFileSync(file, updated, 'utf8');
  }
}
