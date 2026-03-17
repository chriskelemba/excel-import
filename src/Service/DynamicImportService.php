<?php

namespace ChrisKelemba\ExcelImport\Service;

use ChrisKelemba\ExcelImport\Config\Defaults;
use ChrisKelemba\ExcelImport\Core\Exceptions\ImportException;
use ChrisKelemba\ExcelImport\Core\Readers\SpreadsheetReaderManager;
use ChrisKelemba\ExcelImport\Database\DatabaseAdapterInterface;
use ChrisKelemba\ExcelImport\Database\ConnectionManager;
use ChrisKelemba\ExcelImport\Registry\ImportTableRegistry;

class DynamicImportService
{
    private array $config;

    public function __construct(
        private readonly ImportTableRegistry $registry,
        private readonly SpreadsheetReaderManager $reader = new SpreadsheetReaderManager(),
        array $config = []
    ) {
        $this->config = $this->mergeConfig(Defaults::values(), $config);
    }

    public function template(?string $table = null, ?string $connection = null): array
    {
        $resolvedConnection = $this->registry->resolveConnection($connection);

        if ($table !== null) {
            $this->assertImportable($table, $resolvedConnection);

            return [
                'table' => $table,
                'connection' => $resolvedConnection,
                'definition' => $this->registry->definition($table, $resolvedConnection),
            ];
        }

        return [
            'connection' => $resolvedConnection,
            'tables' => $this->registry->all($resolvedConnection),
        ];
    }

    public function databases(?string $connection = null, ?string $table = null): array
    {
        $connections = $this->connectionCatalog();
        $resolvedConnection = $this->registry->resolveConnection($connection);
        $tables = $this->registry->all($resolvedConnection);

        $result = [
            'connections' => $connections,
            'selected_connection' => $resolvedConnection,
            'tables' => $tables,
            'database_preview' => $this->buildDatabasePreview($tables),
        ];

        if (is_string($table) && trim($table) !== '') {
            $this->assertImportable($table, $resolvedConnection);
            $result['selected_table'] = $table;
            $result['definition'] = $this->registry->definition($table, $resolvedConnection);
        }

        return $result;
    }

    public function records(
        string $table,
        ?string $connection = null,
        int $page = 1,
        int $perPage = 25
    ): array {
        $resolvedConnection = $this->registry->resolveConnection($connection);
        $this->assertImportable($table, $resolvedConnection);

        $page = max(1, $page);
        $perPage = max(1, min(200, $perPage));
        $offset = ($page - 1) * $perPage;

        try {
            $query = $this->adapter($resolvedConnection)->fetchRecords($table, $offset, $perPage);
            $total = (int) ($query['total'] ?? 0);
            $rows = (array) ($query['rows'] ?? []);
        } catch (\Throwable $e) {
            throw new ImportException("Failed to load table '{$table}' records: {$e->getMessage()}");
        }

        return [
            'connection' => $resolvedConnection,
            'table' => $table,
            'pagination' => [
                'page' => $page,
                'per_page' => $perPage,
                'total' => $total,
                'total_pages' => $perPage > 0 ? (int) ceil($total / $perPage) : 0,
            ],
            'rows' => $this->normalizeRows($rows),
        ];
    }

    public function preview(
        string $filePath,
        string $originalName,
        string $table,
        array $columnMap = [],
        array $staticValues = [],
        ?int $headerRow = null,
        int $sampleRows = 10,
        int $sheetIndex = 0,
        ?string $connection = null
    ): array {
        $resolvedConnection = $this->registry->resolveConnection($connection);
        $this->assertImportable($table, $resolvedConnection);
        $definition = $this->registry->definition($table, $resolvedConnection);

        $sheet = $this->readSheet($filePath, $originalName, $sheetIndex);
        $rows = $sheet['rows'];
        if ($rows === []) {
            throw new ImportException('Uploaded file has no rows.');
        }

        $headerRow = $headerRow ?? $this->detectHeaderRow($rows);
        $headerCells = $rows[$headerRow - 1] ?? [];
        $headers = $this->buildHeaders($headerCells);

        $resolvedMap = $columnMap !== []
            ? $this->normalizeColumnMap($columnMap)
            : $this->autoMap($headers, $definition['columns']);
        $resolvedStaticValues = $this->resolveStaticValues($staticValues, $definition);

        $validation = $this->validateMapping($resolvedMap, $headers, $definition, $resolvedStaticValues);
        $sampleRows = max(1, min($sampleRows, 50));
        $mappedSample = $this->buildMappedSample(
            $rows,
            $headerRow,
            $resolvedMap,
            $sampleRows,
            (array) ($definition['column_types'] ?? []),
            $resolvedStaticValues
        );

        return [
            'table' => $table,
            'connection' => $resolvedConnection,
            'sheet' => ['name' => $sheet['name'], 'index' => $sheetIndex],
            'header_row' => $headerRow,
            'headers' => $headers,
            'available_columns' => $definition['columns'],
            'required_columns' => $definition['required'],
            'resolved_column_map' => $resolvedMap,
            'resolved_static_values' => $resolvedStaticValues,
            'mapping_validation' => $validation,
            'sample_rows' => $mappedSample,
            'grid' => $this->buildGrid($sheet),
            'can_import' => $validation['errors'] === [],
        ];
    }

    public function run(
        string $filePath,
        string $originalName,
        string $table,
        array $columnMap,
        array $staticValues = [],
        ?int $headerRow = null,
        ?string $mode = null,
        array $uniqueBy = [],
        int $sheetIndex = 0,
        ?string $connection = null
    ): array {
        $resolvedConnection = $this->registry->resolveConnection($connection);
        $this->assertImportable($table, $resolvedConnection);
        $definition = $this->registry->definition($table, $resolvedConnection);

        $sheet = $this->readSheet($filePath, $originalName, $sheetIndex);
        $rows = $sheet['rows'];
        if ($rows === []) {
            throw new ImportException('Uploaded file has no rows.');
        }

        $headerRow = $headerRow ?? $this->detectHeaderRow($rows);
        $headers = $this->buildHeaders($rows[$headerRow - 1] ?? []);
        $resolvedMap = $columnMap !== []
            ? $this->normalizeColumnMap($columnMap)
            : $this->autoMap($headers, $definition['columns']);
        $resolvedStaticValues = $this->resolveStaticValues($staticValues, $definition);
        $validation = $this->validateMapping($resolvedMap, $headers, $definition, $resolvedStaticValues);

        if ($validation['errors'] !== []) {
            throw new ImportException(
                $this->buildRunValidationMessage($validation, $resolvedMap, $headers, $headerRow)
            );
        }

        $mode = in_array($mode, ['insert', 'upsert'], true) ? $mode : $definition['mode'];
        $uniqueBy = $uniqueBy !== [] ? array_values($uniqueBy) : $definition['unique_by'];

        if ($mode === 'upsert' && $uniqueBy === []) {
            throw new ImportException("Import mode 'upsert' requires unique_by columns.");
        }

        $dataRows = array_slice($rows, max(0, $headerRow));
        $result = [
            'rows_processed' => 0,
            'rows_skipped' => 0,
            'entities_written' => 0,
            'connection' => $resolvedConnection,
            'mode' => $mode,
            'unique_by' => $uniqueBy,
            'static_values' => $resolvedStaticValues,
            'errors' => [],
        ];

        foreach ($dataRows as $index => $row) {
            $result['rows_processed']++;
            $rowNumber = $headerRow + 1 + $index;

            $mapped = $this->mapRow($row, $headers, $resolvedMap, (array) ($definition['column_types'] ?? []));
            $mapped = $this->applyStaticValues($mapped, $resolvedStaticValues, (array) ($definition['column_types'] ?? []));
            if ($this->isMappedRowEmpty($mapped)) {
                $result['rows_skipped']++;
                continue;
            }

            $missingRequired = [];
            foreach ($definition['required'] as $requiredColumn) {
                if (!array_key_exists($requiredColumn, $mapped) || $mapped[$requiredColumn] === null || $mapped[$requiredColumn] === '') {
                    $missingRequired[] = $requiredColumn;
                }
            }

            if ($missingRequired !== []) {
                $result['rows_skipped']++;
                $result['errors'][] = "Row {$rowNumber}: missing required column(s): " . implode(', ', $missingRequired);
                continue;
            }

            try {
                if ($mode === 'upsert') {
                    $this->adapter($resolvedConnection)->upsertRow($table, $mapped, $uniqueBy);
                } else {
                    $this->adapter($resolvedConnection)->insertRow($table, $mapped);
                }

                $result['entities_written']++;
            } catch (\Throwable $e) {
                $result['rows_skipped']++;
                $result['errors'][] = "Row {$rowNumber}: " . $this->humanizeDbError($e->getMessage());
            }
        }

        return $result;
    }

    public function runMulti(
        string $filePath,
        string $originalName,
        array $imports,
        ?string $connection = null
    ): array {
        if ($imports === []) {
            throw new ImportException('At least one import definition is required.');
        }

        $globalConnection = $this->registry->resolveConnection($connection);
        $results = [];
        $summary = [
            'rows_processed' => 0,
            'rows_skipped' => 0,
            'entities_written' => 0,
            'imports_count' => 0,
            'errors' => [],
        ];

        foreach ($imports as $index => $import) {
            $item = is_array($import) ? $import : [];
            $table = (string) ($item['table'] ?? '');
            if (trim($table) === '') {
                throw new ImportException("Import item at index {$index} is missing a table.");
            }

            $itemConnection = is_string($item['connection'] ?? null)
                ? (string) $item['connection']
                : $globalConnection;

            $result = $this->run(
                filePath: $filePath,
                originalName: $originalName,
                table: $table,
                columnMap: (array) ($item['column_map'] ?? []),
                staticValues: (array) ($item['static_values'] ?? []),
                headerRow: isset($item['header_row']) ? (int) $item['header_row'] : null,
                mode: is_string($item['mode'] ?? null) ? (string) $item['mode'] : null,
                uniqueBy: (array) ($item['unique_by'] ?? []),
                sheetIndex: isset($item['sheet_index']) ? (int) $item['sheet_index'] : 0,
                connection: $itemConnection
            );

            $summary['imports_count']++;
            $summary['rows_processed'] += (int) ($result['rows_processed'] ?? 0);
            $summary['rows_skipped'] += (int) ($result['rows_skipped'] ?? 0);
            $summary['entities_written'] += (int) ($result['entities_written'] ?? 0);
            foreach ((array) ($result['errors'] ?? []) as $error) {
                $summary['errors'][] = "Import #{$index} ({$table}): {$error}";
            }

            $results[] = [
                'index' => $index,
                'table' => $table,
                'connection' => $itemConnection,
                'result' => $result,
            ];
        }

        return [
            'summary' => $summary,
            'imports' => $results,
        ];
    }

    private function humanizeDbError(string $message): string
    {
        if (stripos($message, 'Incorrect decimal value') !== false) {
            return 'Invalid number format in one of the numeric columns. Remove commas/currency symbols or use standard numeric values.';
        }

        if (stripos($message, 'Invalid datetime format') !== false || stripos($message, 'Incorrect date value') !== false) {
            return 'Invalid date format. Use dates like YYYY-MM-DD, DD-MM-YYYY, or DD/MM/YYYY.';
        }

        if (stripos($message, 'Data too long for column') !== false) {
            return 'One of the values is longer than the destination column allows.';
        }

        if (stripos($message, 'cannot be null') !== false) {
            return 'A required database column received a null value.';
        }

        return $message;
    }

    private function buildRunValidationMessage(
        array $validation,
        array $columnMap,
        array $headers,
        int $headerRow
    ): string {
        $errors = array_values((array) ($validation['errors'] ?? []));
        if ($errors === []) {
            return 'Column mapping has validation errors.';
        }

        $sourceHeaderErrors = array_values(array_filter($errors, static function ($error): bool {
            return is_string($error) && str_starts_with($error, "Source header '");
        }));

        if ($sourceHeaderErrors !== [] && count($sourceHeaderErrors) === count($columnMap)) {
            $headerPreview = implode(', ', array_slice($headers, 0, 10));
            if (count($headers) > 10) {
                $headerPreview .= ', ...';
            }

            return "Mapped headers were not found in header_row={$headerRow}. "
                . 'This usually means the selected header row is incorrect. '
                . "Headers detected at this row: [{$headerPreview}].";
        }

        $previewErrors = implode(' | ', array_slice($errors, 0, 3));
        $remaining = count($errors) - min(3, count($errors));
        if ($remaining > 0) {
            $previewErrors .= " | +{$remaining} more";
        }

        return "Column mapping has validation errors: {$previewErrors}";
    }

    private function assertImportable(string $table, ?string $connection = null): void
    {
        if (!$this->registry->has($table, $connection)) {
            throw new ImportException("Table '{$table}' is not configured as importable.");
        }
    }

    private function readSheet(string $filePath, string $originalName, int $sheetIndex): array
    {
        $workbook = $this->reader->read($filePath, $originalName);
        $sheets = $workbook['sheets'] ?? [];

        if (!isset($sheets[$sheetIndex])) {
            throw new ImportException("Sheet index {$sheetIndex} not found.");
        }

        return $sheets[$sheetIndex];
    }

    private function detectHeaderRow(array $rows): int
    {
        foreach (array_slice($rows, 0, 20) as $index => $row) {
            $nonEmpty = array_values(array_filter($row, static fn ($value): bool => (string) ($value ?? '') !== ''));
            if (count($nonEmpty) >= 2) {
                return $index + 1;
            }
        }

        return 1;
    }

    private function buildHeaders(array $headerCells): array
    {
        $headerCells = $this->trimTrailingEmptyCells($headerCells);
        $headers = [];
        foreach ($headerCells as $index => $value) {
            $label = trim((string) ($value ?? ''));
            $headers[] = $label !== '' ? $label : 'Column ' . $this->indexToColumnLabel($index);
        }

        return $headers;
    }

    private function normalizeColumnMap(array $columnMap): array
    {
        $normalized = [];
        foreach ($columnMap as $source => $target) {
            if (!is_string($source) || trim($source) === '') {
                continue;
            }
            if (!is_string($target) || trim($target) === '') {
                continue;
            }
            $normalized[trim($source)] = trim($target);
        }

        return $normalized;
    }

    private function autoMap(array $headers, array $availableColumns): array
    {
        $map = [];
        $columnByNormalized = [];

        foreach ($availableColumns as $column) {
            $columnByNormalized[$this->normalize($column)] = $column;
        }

        foreach ($headers as $header) {
            $normalizedHeader = $this->normalize($header);
            if (isset($columnByNormalized[$normalizedHeader])) {
                $map[$header] = $columnByNormalized[$normalizedHeader];
            }
        }

        return $map;
    }

    private function validateMapping(array $columnMap, array $headers, array $definition, array $staticValues = []): array
    {
        $errors = [];
        $warnings = [];
        $headerLookup = [];

        foreach ($headers as $header) {
            $headerLookup[$this->normalize($header)] = $header;
        }

        $allowedColumns = (array) ($definition['columns'] ?? []);
        $hasColumnRestrictions = $allowedColumns !== [];

        foreach ($columnMap as $sourceHeader => $targetColumn) {
            if (!isset($headerLookup[$this->normalize($sourceHeader)])) {
                $errors[] = "Source header '{$sourceHeader}' was not found in selected sheet.";
            }

            if ($hasColumnRestrictions && !in_array($targetColumn, $allowedColumns, true)) {
                $errors[] = "Target column '{$targetColumn}' is not allowed for table '{$definition['table']}'.";
            }
        }

        foreach ($staticValues as $targetColumn => $_value) {
            if ($hasColumnRestrictions && !in_array($targetColumn, $allowedColumns, true)) {
                $errors[] = "Static target column '{$targetColumn}' is not allowed for table '{$definition['table']}'.";
            }
        }

        $mappedTargets = array_values($columnMap);
        $mappedTargets = array_values(array_unique(array_merge($mappedTargets, array_keys($staticValues))));
        foreach ((array) ($definition['required'] ?? []) as $requiredColumn) {
            if (!in_array($requiredColumn, $mappedTargets, true)) {
                $errors[] = "Required target column '{$requiredColumn}' is not mapped.";
            }
        }

        if ($columnMap === []) {
            $warnings[] = 'No column mappings provided.';
        }

        if (!$hasColumnRestrictions) {
            $warnings[] = "No schema columns were discovered for table '{$definition['table']}'. Target columns will be accepted dynamically.";
        }

        return ['errors' => array_values(array_unique($errors)), 'warnings' => array_values(array_unique($warnings))];
    }

    private function buildMappedSample(
        array $rows,
        int $headerRow,
        array $columnMap,
        int $sampleRows,
        array $columnTypes = [],
        array $staticValues = []
    ): array {
        $headers = $this->buildHeaders($rows[$headerRow - 1] ?? []);
        $samples = [];
        $dataRows = array_slice($rows, max(0, $headerRow));

        foreach ($dataRows as $offset => $row) {
            $mapped = $this->mapRow($row, $headers, $columnMap, $columnTypes);
            $mapped = $this->applyStaticValues($mapped, $staticValues, $columnTypes);
            if ($this->isMappedRowEmpty($mapped)) {
                continue;
            }

            $samples[] = [
                'row_number' => $headerRow + $offset + 1,
                'raw' => $this->rawRow($row, $headers),
                'mapped' => $mapped,
            ];

            if (count($samples) >= $sampleRows) {
                break;
            }
        }

        return $samples;
    }

    private function rawRow(array $row, array $headers): array
    {
        $raw = [];
        foreach ($headers as $index => $header) {
            $raw[$header] = isset($row[$index]) ? (string) ($row[$index] ?? '') : '';
        }

        return $raw;
    }

    private function mapRow(array $row, array $headers, array $columnMap, array $columnTypes = []): array
    {
        $headerIndexByNormalized = [];
        foreach ($headers as $index => $header) {
            $headerIndexByNormalized[$this->normalize($header)] = $index;
        }

        $mapped = [];
        foreach ($columnMap as $sourceHeader => $targetColumn) {
            $normalizedSource = $this->normalize($sourceHeader);
            if (!isset($headerIndexByNormalized[$normalizedSource])) {
                continue;
            }

            $cellIndex = $headerIndexByNormalized[$normalizedSource];
            $value = $row[$cellIndex] ?? null;
            $value = is_string($value) ? trim($value) : $value;
            $value = ($value === '') ? null : $value;
            $mapped[$targetColumn] = $this->normalizeForColumnType($value, $columnTypes[$targetColumn] ?? null);
        }

        return $mapped;
    }

    private function normalizeForColumnType(mixed $value, ?string $columnType): mixed
    {
        if ($value === null || $columnType === null) {
            return $value;
        }

        if (!is_string($value) && !is_numeric($value)) {
            return $value;
        }

        $type = strtolower(trim($columnType));
        $raw = trim((string) $value);
        if ($raw === '') {
            return null;
        }

        if ($this->isNumericColumnType($type)) {
            $normalized = $this->normalizeNumericString($raw);
            return is_numeric($normalized) ? $normalized : $raw;
        }

        if ($this->isDateOnlyColumnType($type)) {
            $normalizedDate = $this->normalizeDateString($raw, false);
            return $normalizedDate ?? $raw;
        }

        if ($this->isDateTimeColumnType($type)) {
            $normalizedDate = $this->normalizeDateString($raw, true);
            return $normalizedDate ?? $raw;
        }

        return $value;
    }

    private function isNumericColumnType(string $type): bool
    {
        if (str_contains($type, 'int')) {
            return true;
        }

        if (str_contains($type, 'decimal') || str_contains($type, 'numeric')) {
            return true;
        }

        if (str_contains($type, 'float') || str_contains($type, 'double') || str_contains($type, 'real')) {
            return true;
        }

        return in_array($type, [
            'integer',
            'int',
            'bigint',
            'smallint',
            'tinyint',
            'mediumint',
            'decimal',
            'double',
            'float',
            'real',
            'numeric',
        ], true);
    }

    private function isDateOnlyColumnType(string $type): bool
    {
        return $type === 'date';
    }

    private function isDateTimeColumnType(string $type): bool
    {
        if (str_contains($type, 'timestamp')) {
            return true;
        }

        if (str_contains($type, 'datetime')) {
            return true;
        }

        return in_array($type, ['datetime', 'timestamp'], true);
    }

    private function normalizeNumericString(string $value): string
    {
        $value = trim($value);
        $isNegative = false;

        if (preg_match('/^\((.*)\)$/', $value, $matches) === 1) {
            $isNegative = true;
            $value = (string) ($matches[1] ?? '');
        }

        $value = preg_replace('/[\s\$\€\£\¥]/u', '', $value) ?? $value;
        $value = trim($value);

        if (str_ends_with($value, '-')) {
            $isNegative = true;
            $value = substr($value, 0, -1);
        }

        $hasComma = str_contains($value, ',');
        $hasDot = str_contains($value, '.');

        $normalized = $value;
        if ($hasComma && $hasDot) {
            $normalized = str_replace(',', '', $value);
        } elseif ($hasComma && !$hasDot) {
            $parts = explode(',', $value);
            if (count($parts) === 2 && strlen($parts[1]) <= 2) {
                $normalized = $parts[0] . '.' . $parts[1];
            } else {
                $normalized = str_replace(',', '', $value);
            }
        }

        if ($isNegative && !str_starts_with($normalized, '-')) {
            $normalized = '-' . $normalized;
        }

        return $normalized;
    }

    private function normalizeDateString(string $value, bool $withTime): ?string
    {
        $formats = [
            'Y-m-d',
            'd-m-Y',
            'd/m/Y',
            'm/d/Y',
            'm-d-Y',
            'd.m.Y',
            'Y/m/d',
            'Y.m.d',
        ];

        foreach ($formats as $format) {
            $dt = \DateTimeImmutable::createFromFormat('!' . $format, $value);
            if ($dt instanceof \DateTimeImmutable) {
                return $withTime ? $dt->format('Y-m-d H:i:s') : $dt->format('Y-m-d');
            }
        }

        $timestamp = strtotime($value);
        if ($timestamp === false) {
            return null;
        }

        return $withTime
            ? date('Y-m-d H:i:s', $timestamp)
            : date('Y-m-d', $timestamp);
    }

    private function isMappedRowEmpty(array $mapped): bool
    {
        foreach ($mapped as $value) {
            if ($value !== null && $value !== '') {
                return false;
            }
        }

        return true;
    }

    private function buildGrid(array $sheet): array
    {
        $maxRows = (int) $this->getConfig('preview.max_rows', 200);
        $rows = (array) ($sheet['rows'] ?? []);
        $readerGrid = $sheet['grid'] ?? null;
        if (is_array($readerGrid) && isset($readerGrid['rows'], $readerGrid['column_labels'])) {
            $gridRows = array_slice((array) $readerGrid['rows'], 0, max(1, $maxRows));
            return [
                'column_labels' => (array) $readerGrid['column_labels'],
                'rows' => $gridRows,
            ];
        }

        $slice = array_slice($rows, 0, max(1, $maxRows));
        $maxColumns = $this->effectiveColumnCount($slice);

        $columnLabels = [];
        for ($i = 0; $i < $maxColumns; $i++) {
            $columnLabels[] = $this->indexToColumnLabel($i);
        }

        return [
            'column_labels' => $columnLabels,
            'rows' => array_map(function (array $row, int $index) use ($maxColumns): array {
                $cells = [];
                for ($col = 0; $col < $maxColumns; $col++) {
                    $cells[] = ['value' => $row[$col] ?? null];
                }

                return [
                    'row_number' => $index + 1,
                    'cells' => $cells,
                ];
            }, $slice, array_keys($slice)),
        ];
    }

    private function effectiveColumnCount(array $rows): int
    {
        $maxIndex = -1;

        foreach ($rows as $row) {
            if (!is_array($row)) {
                continue;
            }

            for ($i = count($row) - 1; $i >= 0; $i--) {
                $value = $row[$i] ?? null;
                if ($value !== null && trim((string) $value) !== '') {
                    $maxIndex = max($maxIndex, $i);
                    break;
                }
            }
        }

        return $maxIndex + 1;
    }

    private function trimTrailingEmptyCells(array $cells): array
    {
        $lastNonEmpty = -1;
        for ($i = count($cells) - 1; $i >= 0; $i--) {
            $value = $cells[$i] ?? null;
            if ($value !== null && trim((string) $value) !== '') {
                $lastNonEmpty = $i;
                break;
            }
        }

        if ($lastNonEmpty < 0) {
            return [];
        }

        return array_slice($cells, 0, $lastNonEmpty + 1);
    }

    private function indexToColumnLabel(int $index): string
    {
        $index += 1;
        $label = '';

        while ($index > 0) {
            $index--;
            $label = chr(65 + ($index % 26)) . $label;
            $index = intdiv($index, 26);
        }

        return $label;
    }

    private function normalize(string $value): string
    {
        $value = strtolower(trim($value));
        $value = preg_replace('/[^a-z0-9]+/', '_', $value) ?? '';
        return trim($value, '_');
    }

    private function resolveStaticValues(array $staticValues, array $definition): array
    {
        $configValues = $this->normalizeStaticValues((array) ($definition['static_values'] ?? []));
        $requestValues = $this->normalizeStaticValues($staticValues);
        $resolved = array_merge($configValues, $requestValues);

        $columnTypes = (array) ($definition['column_types'] ?? []);
        foreach ($resolved as $column => $value) {
            $resolved[$column] = $this->normalizeForColumnType($value, $columnTypes[$column] ?? null);
        }

        return $resolved;
    }

    private function normalizeStaticValues(array $staticValues): array
    {
        $normalized = [];
        foreach ($staticValues as $column => $value) {
            if (!is_string($column) || trim($column) === '') {
                continue;
            }
            $normalized[trim($column)] = $value;
        }

        return $normalized;
    }

    private function applyStaticValues(array $mapped, array $staticValues, array $columnTypes): array
    {
        if ($staticValues === []) {
            return $mapped;
        }

        foreach ($staticValues as $column => $value) {
            $mapped[$column] = $this->normalizeForColumnType($value, $columnTypes[$column] ?? null);
        }

        return $mapped;
    }

    private function normalizeRows(iterable $rows): array
    {
        $normalized = [];

        foreach ($rows as $row) {
            $arrayRow = is_array($row) ? $row : (array) $row;
            $item = [];

            foreach ($arrayRow as $key => $value) {
                if (!is_string($key) || trim($key) === '') {
                    continue;
                }
                $item[$key] = $this->normalizeValue($value);
            }

            $normalized[] = $item;
        }

        return $normalized;
    }

    private function normalizeValue(mixed $value): mixed
    {
        if ($value === null || is_scalar($value)) {
            return $value;
        }

        if ($value instanceof \DateTimeInterface) {
            return $value->format(DATE_ATOM);
        }

        if (class_exists('MongoDB\\BSON\\ObjectId') && is_a($value, 'MongoDB\\BSON\\ObjectId')) {
            return (string) $value;
        }

        if (class_exists('MongoDB\\BSON\\UTCDateTime') && is_a($value, 'MongoDB\\BSON\\UTCDateTime')) {
            return $value->toDateTime()->format(DATE_ATOM);
        }

        if (is_array($value)) {
            $result = [];
            foreach ($value as $key => $child) {
                $normalizedKey = is_string($key) ? $key : (string) $key;
                $result[$normalizedKey] = $this->normalizeValue($child);
            }

            return $result;
        }

        if (is_object($value)) {
            $arrayObject = (array) $value;
            $result = [];
            foreach ($arrayObject as $key => $child) {
                $normalizedKey = is_string($key) ? $key : (string) $key;
                $result[$normalizedKey] = $this->normalizeValue($child);
            }

            return $result;
        }

        return (string) $value;
    }

    private function connectionCatalog(): array
    {
        return $this->connections()->catalog();
    }

    private function buildDatabasePreview(array $tables): array
    {
        $preview = [];

        foreach ($tables as $tableName => $definition) {
            if (!is_string($tableName) || trim($tableName) === '' || !is_array($definition)) {
                continue;
            }

            $columns = array_values(array_filter((array) ($definition['columns'] ?? []), static function ($column): bool {
                return is_string($column) && trim($column) !== '';
            }));

            $preview[] = [
                'table' => $tableName,
                'columns' => $columns,
                'column_count' => count($columns),
            ];
        }

        usort($preview, static function (array $a, array $b): int {
            return strcasecmp((string) ($a['table'] ?? ''), (string) ($b['table'] ?? ''));
        });

        return [
            'total_tables' => count($preview),
            'items' => $preview,
        ];
    }

    private function getConfig(string $path, mixed $default = null): mixed
    {
        $segments = explode('.', $path);
        $value = $this->config;

        foreach ($segments as $segment) {
            if (!is_array($value) || !array_key_exists($segment, $value)) {
                return $default;
            }
            $value = $value[$segment];
        }

        return $value;
    }

    private function mergeConfig(array $base, array $override): array
    {
        foreach ($override as $key => $value) {
            if (is_array($value) && isset($base[$key]) && is_array($base[$key])) {
                $base[$key] = $this->mergeConfig($base[$key], $value);
                continue;
            }

            $base[$key] = $value;
        }

        return $base;
    }

    private function adapter(string $connection): DatabaseAdapterInterface
    {
        return $this->connections()->get($connection);
    }

    private function connections(): ConnectionManager
    {
        return $this->registry->connections();
    }
}
