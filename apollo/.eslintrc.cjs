module.exports = {
  root: true,
  env: { browser: true, es2022: true },
  extends: [
    'eslint:recommended',
    'plugin:@typescript-eslint/recommended',
    'plugin:unicorn/all',
    'plugin:@typescript-eslint/recommended',
    'plugin:import/recommended',
    ''
  ],
  ignorePatterns: [
    'node_modules/',
    'build/',
    'dist/'
  ],
  parser: '@typescript-eslint/parser',
  parserOptions: {
    ecmaVersion: 'latest',
    sourceType: 'module'
  },
  plugins: [
    'unicorn',
    '@typescript-eslint',
    'import'
  ],
  settings: {
    'import/resolver': {
      node: {
        "extensions": [".ts", ".tsx", ".d.ts"],
        "moduleDirectory": ["src", "node_modules"]
      }
    },
  }
};
