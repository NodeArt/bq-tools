import fs from 'fs/promises';
import path from 'path';
import { TableSchema } from './bigQueryService';

type ICfgParams<T, R extends boolean | undefined> = ({ envName: string } | { argName: string }) & {
  envName?: string;
  argName?: string;
  required?: R;
  fallback?: T;
};

type CfgFn<T> = {
  (params: ICfgParams<undefined, false | undefined>): T | undefined;
  (params: ICfgParams<undefined, true>): T;
  (params: ICfgParams<T, boolean>): T;
};

export class ConfigError extends Error {
  constructor(
    message: string,
    public readonly envName: string | undefined,
    public readonly argName: string | undefined,
  ) {
    super(message);
  }
}

export const cfgString: CfgFn<string> = ({ envName, argName, required, fallback }) => {
  if (!envName && !argName) {
    throw new ConfigError('At least one of env or arg parameter names must be specified', envName, argName);
  }

  const envVal = envName ? process.env[envName] : undefined;
  const argVal = argName
    ? process.argv
        .filter((arg) => arg.startsWith(`--${argName}=`))
        .map((arg) => arg.slice(argName.length + 3))
        .at(-1)
    : undefined;

  const val = argVal || envVal || fallback;

  if (required && !val) {
    throw new ConfigError('Required config parameter not set', envName, argName);
  }

  return val as string;
};

export const readJsonFile = async <T>(filePath: string): Promise<T> => {
  const data = await fs.readFile(filePath, { encoding: 'utf8' });

  return JSON.parse(data);
};

export const getDirSchemas = async (dir: string): Promise<Array<{ name: string; schema: TableSchema }>> => {
  const files = await fs.readdir(dir, { withFileTypes: true });

  return Promise.all(
    files.flatMap(async (f) => {
      if (f.isFile() && f.name.endsWith('.schema.json')) {
        const schema = await readJsonFile<TableSchema>(path.resolve(dir, f.name));

        return [{ name: path.basename(f.name, '.schema.json'), schema }];
      }

      return [];
    }),
  ).then((arr) => arr.flat());
};
