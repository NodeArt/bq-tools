import { AuthService, ICredentials } from './authService';
import { executeWithBackoffRetry, IBackoffRetryOptions } from './utils';

// Note: putting BigQuery types here to avoid extra dependencies.
// Ref: https://github.com/googleapis/nodejs-bigquery/blob/main/src/types.d.ts
export type TableFieldSchema = {
  categories?: { names?: Array<string> };
  collation?: string;
  dataPolicies?: Array<{ name?: string }>;
  defaultValueExpression?: string;
  description?: string;
  fields?: Array<TableFieldSchema>;
  foreignTypeDefinition?: string;
  maxLength?: string;
  mode?: string;
  name?: string;
  policyTags?: { names?: Array<string> };
  precision?: string;
  rangeElementType?: { type?: string };
  roundingMode?: string;
  scale?: string;
  type?: string;
};
export type TableSchema = {
  fields?: Array<TableFieldSchema>;
  foreignTypeInfo?: { typeSystem?: string };
};

export interface IBigQueryErrorResponse {
  error: {
    code: number;
    message: string;
    errors: {
      message: string;
      domain: string;
      reason: string;
    }[];
    status: string;
  };
}

export interface IBigQueryInsertError {
  index: number;
  errors: {
    reason: string;
    location: string;
    debugInfo: string;
    message: string;
  }[];
}

export interface IBigQueryInsertResponse {
  kind: string;
  insertErrors?: IBigQueryInsertError[];
}

export interface IQueryOptions {
  parameters?: Record<string, unknown>;
  maxResults?: number;
  timeoutMs?: number;
}

export class BigQueryError extends Error {
  public readonly code: number;
  public readonly errors?: Array<object>;
  public readonly status: string;

  constructor(resp: IBigQueryErrorResponse) {
    super(resp.error.message);
    this.code = resp.error.code;
    this.errors = resp.error.errors;
    this.status = resp.error.status;
  }
}

export class BigQueryInsertError extends Error {
  constructor(
    public readonly datasetId: string,
    public readonly tableId: string,
    public readonly errors: IBigQueryInsertError[],
  ) {
    super('Failed to insert into table');
  }
}

export class BigQueryService {
  protected readonly scopes = ['https://www.googleapis.com/auth/bigquery'];
  protected readonly projectId;
  protected readonly authService;

  constructor(credentials: ICredentials, projectId?: string) {
    this.projectId = projectId ?? credentials.project_id;
    this.authService = new AuthService(credentials);
  }

  protected async request<T>(path: string, method: string, body?: object): Promise<T> {
    const { accessToken } = await this.authService.getAuthToken(this.scopes);
    let resp;
    let data;
    try {
      const init = {
        method,
        headers: {
          Authorization: `Bearer ${accessToken}`,
          Accept: 'application/json',
          'Content-Type': 'application/json',
        },
        body: body ? JSON.stringify(body) : undefined,
      };
      resp = await fetch(`https://bigquery.googleapis.com/bigquery/v2/projects/${this.projectId}${path}`, init);
      data = await resp.text();
    } catch (error) {
      throw new Error('Failed to perform request', { cause: error });
    }

    if (resp.ok) {
      try {
        return JSON.parse(data) as T;
      } catch (error) {
        throw new Error('Failed to parse response', { cause: error });
      }
    }

    let res: IBigQueryErrorResponse;
    try {
      res = JSON.parse(data);
    } catch (error) {
      throw new Error('Failed to parse error response', { cause: error });
    }

    if (res.error) {
      throw new BigQueryError(res);
    } else {
      throw new Error('Request failed');
    }
  }

  protected formatQueryParameters(params?: Record<string, unknown>) {
    return Object.entries(params ?? {}).map(([name, value]) => {
      const vType = typeof value;
      let type;
      if (vType === 'boolean') {
        type = 'BOOL';
      } else if (vType === 'string') {
        type = 'STRING';
      } else if (vType === 'number') {
        type = 'INT64';
      } else if (vType === 'object' && value instanceof Date) {
        type = 'TIMESTAMP';
      } else {
        throw new Error('Unsupported value type');
      }

      return {
        name,
        parameterType: { type },
        parameterValue: { value: `${value}` },
      };
    });
  }

  public async createDatasetIfNotExists(datasetId: string) {
    try {
      await this.request(`/datasets/${datasetId}`, 'GET');

      return;
    } catch (error) {
      if (!(error instanceof BigQueryError) || error.code !== 404) {
        throw error;
      }
    }

    const body = { datasetReference: { datasetId } };
    await this.request('/datasets', 'POST', body);
  }

  public async createOrUpdateTableSchema(datasetId: string, tableId: string, schema: TableSchema) {
    let exists = false;
    try {
      await this.request(`/datasets/${datasetId}/tables/${tableId}`, 'GET');
      exists = true;
    } catch (error) {
      if (!(error instanceof BigQueryError) || error.code !== 404) {
        throw error;
      }
    }

    const body = {
      tableReference: {
        datasetId,
        projectId: this.projectId,
        tableId,
      },
      schema,
    };
    if (exists) {
      await this.request(`/datasets/${datasetId}/tables/${tableId}`, 'PATCH', body);
    } else {
      await this.request(`/datasets/${datasetId}/tables`, 'POST', body);
    }
  }

  public async insert(
    datasetId: string,
    tableId: string,
    rows: Array<Record<string, unknown>>,
    retryOptions?: IBackoffRetryOptions,
  ) {
    const body = { rows: rows.map((r) => ({ json: r })) };

    return executeWithBackoffRetry(async () => {
      const res = await this.request<IBigQueryInsertResponse>(
        `/datasets/${datasetId}/tables/${tableId}/insertAll`,
        'POST',
        body,
      );
      if (res.insertErrors) {
        throw new BigQueryInsertError(datasetId, tableId, res.insertErrors);
      }
    }, retryOptions);
  }

  public async queryRaw<T>(query: string, options?: IQueryOptions): Promise<T> {
    const params = this.formatQueryParameters(options?.parameters);
    const body = {
      query,
      maxResults: options?.maxResults,
      timeoutMs: options?.timeoutMs ?? 100000,
      queryParameters: params.length ? params : undefined,
      useLegacySql: false,
      useQueryCache: true,
    };

    return this.request<T>('/queries', 'POST', body);
  }
}
