<?php

namespace ChrisKelemba\ExcelImport\Workflow;

use ChrisKelemba\ExcelImport\Core\Exceptions\ImportException;
use ChrisKelemba\ExcelImport\Service\DynamicImportService;

class ImportWorkflow
{
    public function __construct(private readonly DynamicImportService $service)
    {
    }

    public function previewFromPayload(string $filePath, string $originalName, array|string $payload): array
    {
        $resolved = $this->resolvePayload($payload);
        $imports = $this->resolveImports($resolved);

        $previews = [];
        foreach ($imports as $index => $import) {
            $previews[] = [
                'index' => $index,
                'table' => $import['table'],
                'preview' => $this->service->preview(
                    filePath: $filePath,
                    originalName: $originalName,
                    table: $import['table'],
                    columnMap: $import['column_map'],
                    staticValues: $import['static_values'],
                    headerRow: $import['header_row'],
                    sampleRows: $import['sample_rows'],
                    sheetIndex: $import['sheet_index'],
                    connection: $import['connection']
                ),
            ];
        }

        return [
            'message' => 'Preview generated.',
            'imports' => $previews,
        ];
    }

    public function runFromPayload(string $filePath, string $originalName, array|string $payload): array
    {
        $resolved = $this->resolvePayload($payload);
        $imports = $this->resolveImports($resolved);
        $connection = is_string($resolved['connection'] ?? null) ? (string) $resolved['connection'] : null;

        return $this->service->runMulti(
            filePath: $filePath,
            originalName: $originalName,
            imports: array_map(static function (array $import): array {
                return [
                    'table' => $import['table'],
                    'connection' => $import['connection'],
                    'column_map' => $import['column_map'],
                    'static_values' => $import['static_values'],
                    'header_row' => $import['header_row'],
                    'mode' => $import['mode'],
                    'unique_by' => $import['unique_by'],
                    'sheet_index' => $import['sheet_index'],
                ];
            }, $imports),
            connection: $connection
        );
    }

    private function resolvePayload(array|string $payload): array
    {
        if (is_array($payload)) {
            return $payload;
        }

        $decoded = json_decode($payload, true);
        if (!is_array($decoded)) {
            throw new ImportException('Payload must be an array or valid JSON object.');
        }

        return $decoded;
    }

    private function resolveImports(array $payload): array
    {
        $imports = $payload['imports'] ?? null;
        if (is_string($imports)) {
            $decoded = json_decode($imports, true);
            if (!is_array($decoded)) {
                throw new ImportException('`imports` must be a valid JSON array.');
            }
            $imports = $decoded;
        }

        if (!is_array($imports) || $imports === []) {
            throw new ImportException('`imports` is required and must be a non-empty array.');
        }

        $result = [];

        foreach ($imports as $index => $import) {
            if (!is_array($import)) {
                throw new ImportException("Import item at index {$index} must be an object.");
            }

            $table = trim((string) ($import['table'] ?? ''));
            if ($table === '') {
                throw new ImportException("Import item at index {$index} is missing `table`.");
            }

            $columnMap = $this->arrayValue($import, ['column_map', 'columnMap']);

            $result[] = [
                'table' => $table,
                'connection' => $this->stringValue($import, ['connection']),
                'column_map' => $columnMap,
                'static_values' => $this->arrayValue($import, ['static_values', 'staticValues']),
                'header_row' => $this->intValue($import, ['header_row', 'headerRow']),
                'sample_rows' => $this->intValue($import, ['sample_rows', 'sampleRows']) ?? 10,
                'mode' => $this->stringValue($import, ['mode']),
                'unique_by' => array_values($this->arrayValue($import, ['unique_by', 'uniqueBy'])),
                'sheet_index' => $this->intValue($import, ['sheet_index', 'sheetIndex']) ?? 0,
            ];
        }

        return $result;
    }

    private function arrayValue(array $source, array $keys): array
    {
        foreach ($keys as $key) {
            if (array_key_exists($key, $source) && is_array($source[$key])) {
                return $source[$key];
            }
        }

        return [];
    }

    private function intValue(array $source, array $keys): ?int
    {
        foreach ($keys as $key) {
            if (array_key_exists($key, $source) && $source[$key] !== null && $source[$key] !== '') {
                return (int) $source[$key];
            }
        }

        return null;
    }

    private function stringValue(array $source, array $keys): ?string
    {
        foreach ($keys as $key) {
            if (!array_key_exists($key, $source) || $source[$key] === null) {
                continue;
            }

            $value = trim((string) $source[$key]);
            if ($value !== '') {
                return $value;
            }
        }

        return null;
    }
}
