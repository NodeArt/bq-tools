# BigQuery Tools

This package consists of 2 parts:
1. Light-weight CLI with minimum dependencies to sync BQ table schemas as one of the steps during deployment.
2. Small BQ client that could be used in a limited [Cloudflare Workers](https://developers.cloudflare.com/workers/) environment.

## CLI

Configuration can be provided via environment variables, command line arguments or a mix of both with command line arguments having priority over environment variables.

Credentials must be specified either via file path in `GOOGLE_APPLICATION_CREDENTIALS` or as an encoded value in `ADC_ENCODED`.

Schemas to be synced must be located in directory specified in `schemas-path` parameter and named in the following format: `<table name>.schema.json` (e.g. `user.schema.json`). File content must be a valid JSON that satisfies `TableSchema` structure. Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#TableSchema.

Note that prefix/suffix will be concatenated as is without adding extra separators since BigQuery supports multiple special characters that could be used as separators in tables' names. Ref: https://cloud.google.com/bigquery/docs/tables#table_naming.

Usage example:
```sh
npx nodeart-bq-cli --project=my_project --dataset=my_dataset --schemas-path=./schemas --table-prefix=project1_ --table-suffix=_stage
```

|                env               |        arg       |  type  | required |            example           |                                               description                                               |
|:--------------------------------:|:----------------:|:------:|:--------:|:----------------------------:|:-------------------------------------------------------------------------------------------------------:|
| `BQ_PROJECT`                     | `--project`      | string |    NO    | `my_project`                 | Google Cloud Project ID. If not set, it will be taken from credentials.                                 |
| `BQ_DATASET`                     | `--dataset`      | string |    YES   | `my_dataset`                 | Dataset ID. Ref: https://cloud.google.com/bigquery/docs/datasets#dataset-naming                         |
| `BQ_SCHEMAS_PATH`                | `--schemas-path` | string |    YES   | `./path/to/schemas`          | Path to schemas directory                                                                               |
| `GOOGLE_APPLICATION_CREDENTIALS` |         -        | string |    NO    | `./path/to/credentials.json` | Path to ADC file. Ref: https://cloud.google.com/docs/authentication/application-default-credentials#GAC |
| `ADC_ENCODED`                    |         -        | string |    NO    | -                            | Base64-encoded JSON value of Application Default Credentials                                            |
| `BQ_TABLE_PREFIX`                | `--table-prefix` | string |    NO    | `some_prefix_`               | Prefix that will be added to the beginning of tables' names when syncing their schemas                  |
| `BQ_TABLE_SUFFIX`                | `--table-suffix` | string |    NO    | `_and_suffix`                | Suffix that will be added to the end of tables' names when syncing their schemas                        |
