<?php

namespace ChrisKelemba\ExcelImport\Core\Readers;

use ChrisKelemba\ExcelImport\Core\Exceptions\ImportException;
use ChrisKelemba\ExcelImport\Core\Readers\SpreadsheetReaderInterface;

class JsonSpreadsheetReader implements SpreadsheetReaderInterface
{
    public function supports(string $extension): bool
    {
        return in_array(strtolower($extension), ['json', 'ndjson', 'jsonl'], true);
    }

    public function read(string $path): array
    {
        if (!is_readable($path)) {
            throw new ImportException('JSON file is not readable.');
        }

        $contents = trim((string) file_get_contents($path));
        if ($contents === '') {
            throw new ImportException('JSON file is empty.');
        }

        $decoded = $this->decodeJsonPayload($contents);
        $records = $this->extractRecords($decoded);

        if ($records === []) {
            return [
                'sheets' => [
                    [
                        'name' => 'Sheet1',
                        'rows' => [],
                    ],
                ],
            ];
        }

        $headers = [];
        $normalizedRecords = [];

        foreach ($records as $record) {
            $normalized = $this->normalizeRecord($record);
            $normalizedRecords[] = $normalized;

            foreach (array_keys($normalized) as $key) {
                if (!in_array($key, $headers, true)) {
                    $headers[] = $key;
                }
            }
        }

        if ($headers === []) {
            $headers = ['value'];
        }

        $rows = [$headers];
        foreach ($normalizedRecords as $record) {
            $row = [];
            foreach ($headers as $header) {
                $row[] = $record[$header] ?? null;
            }
            $rows[] = $row;
        }

        return [
            'sheets' => [
                [
                    'name' => 'Sheet1',
                    'rows' => $rows,
                ],
            ],
        ];
    }

    private function decodeJsonPayload(string $contents): mixed
    {
        try {
            return json_decode($contents, true, 512, JSON_THROW_ON_ERROR);
        } catch (\JsonException) {
            $ndjson = $this->decodeNdjson($contents);
            if ($ndjson !== null) {
                return $ndjson;
            }

            throw new ImportException('Invalid JSON file. Use a JSON array/object or NDJSON lines.');
        }
    }

    private function decodeNdjson(string $contents): ?array
    {
        $lines = preg_split('/\R/u', $contents) ?: [];
        $records = [];

        foreach ($lines as $line) {
            $line = trim($line);
            if ($line === '') {
                continue;
            }

            try {
                $records[] = json_decode($line, true, 512, JSON_THROW_ON_ERROR);
            } catch (\JsonException) {
                return null;
            }
        }

        return $records === [] ? null : $records;
    }

    private function extractRecords(mixed $decoded): array
    {
        if (is_array($decoded)) {
            if (array_is_list($decoded)) {
                return $decoded;
            }

            $rows = $decoded['rows'] ?? $decoded['data'] ?? null;
            if (is_array($rows) && array_is_list($rows)) {
                return $rows;
            }

            return [$decoded];
        }

        if (is_object($decoded)) {
            return [(array) $decoded];
        }

        return [['value' => $decoded]];
    }

    private function normalizeRecord(mixed $record): array
    {
        if (is_object($record)) {
            $record = (array) $record;
        }

        if (!is_array($record)) {
            return ['value' => $this->normalizeValue($record)];
        }

        if (array_is_list($record)) {
            $normalized = [];
            foreach ($record as $index => $value) {
                $normalized['Column ' . ($index + 1)] = $this->normalizeValue($value);
            }
            return $normalized;
        }

        $normalized = [];
        foreach ($record as $key => $value) {
            if (!is_string($key) && !is_int($key)) {
                continue;
            }

            $column = trim((string) $key);
            if ($column === '') {
                continue;
            }

            $normalized[$column] = $this->normalizeValue($value);
        }

        if ($normalized === []) {
            return ['value' => null];
        }

        return $normalized;
    }

    private function normalizeValue(mixed $value): mixed
    {
        if ($value === null || is_scalar($value)) {
            return $value;
        }

        if (is_array($value)) {
            $result = [];
            foreach ($value as $k => $v) {
                $result[$k] = $this->normalizeValue($v);
            }
            return $result;
        }

        if (is_object($value)) {
            if (method_exists($value, 'jsonSerialize')) {
                return $this->normalizeValue($value->jsonSerialize());
            }

            return $this->normalizeValue((array) $value);
        }

        return (string) $value;
    }
}
