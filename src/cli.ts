import { AuthService, ICredentials } from './authService';
import { BigQueryNodeService } from './bigQueryNodeService';
import { cfgString, getDirSchemas, readJsonFile } from './cfgUtils';

const config = {
  project: cfgString({ envName: 'BQ_PROJECT', argName: 'project' }),
  dataset: cfgString({ envName: 'BQ_DATASET', argName: 'dataset', required: true }),
  schemasPath: cfgString({ envName: 'BQ_SCHEMAS_PATH', argName: 'schemas-path', required: true }),
  tablesPrefix: cfgString({ envName: 'BQ_TABLE_PREFIX', argName: 'table-prefix' }),
  tablesSuffix: cfgString({ envName: 'BQ_TABLE_SUFFIX', argName: 'table-suffix' }),

  credentialsPath: cfgString({ envName: 'GOOGLE_APPLICATION_CREDENTIALS', required: false }),
  credentialsEncoded: cfgString({ envName: 'ADC_ENCODED', required: false }),
};

void (async () => {
  let credentials;
  if (config.credentialsEncoded) {
    credentials = AuthService.decodeCredentials(config.credentialsEncoded);
  } else if (config.credentialsPath) {
    credentials = await readJsonFile<ICredentials>(config.credentialsPath);
  } else {
    throw new Error('Credentials must be specified');
  }

  const bq = new BigQueryNodeService(credentials, config.project);
  const schemas = await getDirSchemas(config.schemasPath);

  console.log(`Syncing dataset '${config.dataset}'`);
  await bq.createDatasetIfNotExists(config.dataset);

  for (const { name, schema } of schemas) {
    const tableId = `${config.tablesPrefix ?? ''}${name}${config.tablesSuffix ?? ''}`;

    console.log(`Syncing table '${config.dataset}.${tableId}'`);
    await bq.createOrUpdateTableSchema(config.dataset, tableId, schema);
  }
})();
