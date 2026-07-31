import eslint from '@eslint/js';
import tsParser from '@typescript-eslint/parser';
import eslintConfigPrettier from 'eslint-config-prettier/flat';
import importPlugin from 'eslint-plugin-import';
import eslintPluginPrettierRecommended from 'eslint-plugin-prettier/recommended';
import globals from 'globals';
import tsEslint, { configs as tsEslintConfigs } from 'typescript-eslint';

export default tsEslint.config([
  {
    ignores: ['**/build/', 'bin/'],
  },
  eslint.configs.recommended,
  tsEslintConfigs.recommended,
  importPlugin.flatConfigs.recommended,
  importPlugin.flatConfigs.typescript,
  eslintPluginPrettierRecommended,
  eslintConfigPrettier,
  // Note: Unfortunately, typescript-related imports causes issues in
  // `eslint-plugin-import` for non-TS files including config files
  // themselves, so we have to split configuration.
  {
    languageOptions: {
      globals: globals.node,
    },
    rules: {
      curly: 'error',
      'newline-before-return': 'error',
      'object-shorthand': 'error',

      'import/order': [
        'error',
        {
          groups: ['builtin', 'external', 'internal', 'parent', 'sibling', 'index', 'object'],
          alphabetize: {
            order: 'asc',
            orderImportKind: 'ignore',
            caseInsensitive: true,
          },
          named: true,
          'newlines-between': 'never',
        },
      ],
    },
  },
  {
    files: ['**/*.config.mjs'],
    settings: {
      'import/resolver': {
        typescript: true,
        node: true,
      },
    },
  },
  {
    files: ['src/**/*.ts'],
    languageOptions: {
      ecmaVersion: 2022,
      globals: globals.node,
      parser: tsParser,
      parserOptions: {
        ecmaVersion: 2022,
        project: './tsconfig.json',
        tsconfigRootDir: import.meta.dirname,
      },
    },
    settings: {
      'import/resolver': {
        typescript: true,
        node: true,
      },
    },
    rules: {
      '@typescript-eslint/explicit-member-accessibility': [
        'error',
        {
          overrides: {
            constructors: 'no-public',
          },
        },
      ],
      '@typescript-eslint/no-base-to-string': 'error',
      '@typescript-eslint/no-explicit-any': 'error',
      '@typescript-eslint/no-floating-promises': 'error',
      '@typescript-eslint/no-inferrable-types': 'error',
      '@typescript-eslint/no-non-null-assertion': 'error',
      '@typescript-eslint/no-unused-vars': [
        'error',
        {
          argsIgnorePattern: '^_',
          varsIgnorePattern: '^_',
          caughtErrorsIgnorePattern: '^_',
        },
      ],
      '@typescript-eslint/return-await': 'error',
    },
  },
]);
