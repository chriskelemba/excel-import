# Excel Import (Framework-Agnostic PHP)

Standalone CSV/XLSX/JSON/NDJSON preview + import engine for PHP.

This is the same core functionality as `excel-import-laravel`, extracted so it can run in:
- plain PHP projects
- Laravel (service/controller wrapper in app code)
- Yii (service/controller wrapper in app code)

## Install

```bash
composer require chriskelemba/excel-import
```

Optional:

```bash
composer require phpoffice/phpspreadsheet   # XLSX support
composer require mongodb/mongodb            # MongoDB support
```

## Database Support

Supported through `PDO` adapters:
- MySQL / MariaDB (`pdo_mysql`)
- PostgreSQL (`pdo_pgsql`)
- SQLite (`pdo_sqlite`)
- SQL Server (`pdo_sqlsrv`)

Supported through Mongo adapter:
- MongoDB (`mongodb/mongodb` package)

Notes:
- Register one or many connections and switch by `connection` parameter in service calls.
- `insert` and `upsert` modes work across the supported SQL drivers and MongoDB.
- Column/type discovery is automatic for SQL tables.
- MongoDB is schema-less; columns are inferred from sampled documents unless overridden in config.

## Quick Start (Plain PHP + PDO)

```php
<?php

use ChrisKelemba\ExcelImport\DynamicImporter;

$pdo = new PDO('mysql:host=127.0.0.1;dbname=test', 'root', 'secret');

$importer = (new DynamicImporter(config: [
    'connection' => 'mysql',
    'discovery' => [
        'allow_unconfigured_tables' => true,
        'allow_tables' => [],
        'exclude_tables' => ['migrations'],
    ],
    'tables' => [
        'skills' => [
            'required' => ['name'],
            'unique_by' => ['name'],
            'mode' => 'upsert',
        ],
    ],
]))
    ->addPdoConnection('mysql', $pdo)
    ->service();

$preview = $importer->preview(
    filePath: __DIR__ . '/skills.xlsx',
    originalName: 'skills.xlsx',
    table: 'skills',
    columnMap: ['Skill Name' => 'name'],
    staticValues: ['ctg_id' => 2]
);

$result = $importer->run(
    filePath: __DIR__ . '/skills.xlsx',
    originalName: 'skills.xlsx',
    table: 'skills',
    columnMap: ['Skill Name' => 'name'],
    mode: 'upsert',
    uniqueBy: ['name']
);
```

## PostgreSQL Example

```php
<?php

use ChrisKelemba\ExcelImport\DynamicImporter;

$pdo = new PDO('pgsql:host=127.0.0.1;port=5432;dbname=test', 'postgres', 'secret');

$service = (new DynamicImporter(config: ['connection' => 'pgsql']))
    ->addPdoConnection('pgsql', $pdo)
    ->service();
```

## MongoDB Connection

```php
$mongo = new MongoDB\Client('mongodb://127.0.0.1:27017');

$service = (new DynamicImporter(config: ['connection' => 'mongodb']))
    ->addMongoConnection('mongodb', $mongo, 'my_database')
    ->service();
```

## Public Service API

- `template(?string $table = null, ?string $connection = null): array`
- `databases(?string $connection = null, ?string $table = null): array`
- `records(string $table, ?string $connection = null, int $page = 1, int $perPage = 25): array`
- `preview(string $filePath, string $originalName, string $table, array $columnMap = [], array $staticValues = [], ?int $headerRow = null, int $sampleRows = 10, int $sheetIndex = 0, ?string $connection = null): array`
- `run(string $filePath, string $originalName, string $table, array $columnMap, array $staticValues = [], ?int $headerRow = null, ?string $mode = null, array $uniqueBy = [], int $sheetIndex = 0, ?string $connection = null): array`
- `runMulti(string $filePath, string $originalName, array $imports, ?string $connection = null): array`

## Behavior Parity Notes

- Same file readers: CSV/TXT, XLSX, JSON/NDJSON
- Same header detection, auto-mapping, validation, sample mapping, and import modes (`insert`/`upsert`)
- Same static values support and numeric/date normalization
- Same multi-table import flow (`runMulti`)
- Same Mongo schema inference behavior (top-level keys sampled from documents)

## Laravel/Yii Usage

Wrap this package inside your framework controller/service:
- Convert uploaded file to `filePath` + `originalName`
- Pass request payload into `preview`, `run`, or `runMulti`
- Return JSON response in your framework format

No framework dependency is required by this package.

## Route-Only Integration (No Custom Controller Logic)

If you want consumers to only define routes, use the HTTP actions bridge:

```php
<?php

use ChrisKelemba\ExcelImport\DynamicImporter;

$actions = (new DynamicImporter(config: ['connection' => 'mysql']))
    ->http();
```

In Laravel, if no connection is manually registered, `DynamicImporter` auto-resolves the framework DB connection (from `config('database.default')` / `.env`) and uses it.

Then bind routes directly:

```php
// Laravel
Route::get('/imports/template', [$actions, 'template']);
Route::get('/imports/databases', [$actions, 'databases']);
Route::get('/imports/records', [$actions, 'records']);
Route::post('/imports/preview', [$actions, 'preview']);
Route::post('/imports/run', [$actions, 'run']);
```

```php
// Yii
$actions = /* build as above */;

$app->get('/imports/template', fn () => $actions->template(Yii::$app->request));
$app->get('/imports/databases', fn () => $actions->databases(Yii::$app->request));
$app->get('/imports/records', fn () => $actions->records(Yii::$app->request));
$app->post('/imports/preview', fn () => $actions->preview(Yii::$app->request));
$app->post('/imports/run', fn () => $actions->run(Yii::$app->request));
```

```php
// Plain PHP (any router)
$actions = /* build as above */;
$result = $actions->run(); // reads body/query/files from globals when request is omitted
```

Payload/file handling supported by the bridge:
- `imports` (array or JSON string), or single-import payload with `table`
- `file` upload from request/`$_FILES`, or explicit `file_path` + optional `original_name`

## Minimal Consumer Workflow

For minimal controller code, use the workflow helper:

```php
<?php

use ChrisKelemba\ExcelImport\DynamicImporter;

$workflow = (new DynamicImporter(config: ['connection' => 'mysql']))
    ->addPdoConnection('mysql', $pdo)
    ->workflow();

$preview = $workflow->previewFromPayload(
    filePath: $filePath,
    originalName: $originalName,
    payload: $requestPayload // array or JSON string with `imports`
);

$result = $workflow->runFromPayload(
    filePath: $filePath,
    originalName: $originalName,
    payload: $requestPayload // array or JSON string with `imports`
);
```

`imports` supports snake_case and camelCase keys:
- `column_map` / `columnMap`
- `static_values` / `staticValues`
- `header_row` / `headerRow`
- `sample_rows` / `sampleRows`
- `unique_by` / `uniqueBy`
- `sheet_index` / `sheetIndex`
