// Standalone ESM Jest config for this repo. Run via:
//   NODE_OPTIONS=--experimental-vm-modules jest --config jest.config.mjs
// (the `npm test` script already does this).
//
// No `transform` key — every spec here is native ESM (`.mjs`), executed
// directly under Node's `--experimental-vm-modules` flag rather than through
// Babel/ts-jest.
export default {
  testEnvironment: 'node',
  testMatch: ['<rootDir>/scripts/**/*.jest.spec.mjs'],
  moduleFileExtensions: ['mjs', 'js'],
  collectCoverageFrom: [
    // lib/ modules are unit/integration-tested directly; cli.mjs is the
    // largely-untested entrypoint, black-box tested via child_process spawns
    // in cli.jest.spec.mjs and its beat-type-specific sibling specs.
    'scripts/agent-resource-management/lib/**/*.mjs',
    '!scripts/agent-resource-management/**/*.jest.spec.mjs',
    '!scripts/agent-resource-management/cli.mjs',
    // The ARM PreToolUse hook is unit-tested via pretooluse-arm-gate.jest.spec.mjs
    // and acceptance-tested end-to-end via stdin/exit-code in its outer-acceptance spec.
    'scripts/agent-resource-management/hooks/**/*.mjs',
    '!scripts/agent-resource-management/hooks/**/*.jest.spec.mjs',
    // The diagnostics collector is black-box tested via child_process.
    'scripts/agent-resource-management/diagnostics/**/*.mjs',
    '!scripts/agent-resource-management/diagnostics/**/*.jest.spec.mjs',
  ],
  coveragePathIgnorePatterns: ['/node_modules/'],
};
