<?php

namespace ChrisKelemba\ExcelImport\Core\Readers;

use ChrisKelemba\ExcelImport\Core\Exceptions\ImportException;
use ChrisKelemba\ExcelImport\Core\Readers\SpreadsheetReaderInterface;

class CsvSpreadsheetReader implements SpreadsheetReaderInterface
{
    public function supports(string $extension): bool
    {
        return in_array(strtolower($extension), ['csv', 'txt'], true);
    }

    public function read(string $path): array
    {
        if (!is_readable($path)) {
            throw new ImportException('CSV file is not readable.');
        }

        $handle = fopen($path, 'rb');
        if ($handle === false) {
            throw new ImportException('Failed to open CSV file.');
        }

        $firstLine = fgets($handle) ?: '';
        rewind($handle);
        $delimiter = $this->detectDelimiter($firstLine);

        $rows = [];
        while (($row = fgetcsv($handle, 0, $delimiter)) !== false) {
            $rows[] = array_map(static function ($value) {
                if ($value === null) {
                    return null;
                }
                $trimmed = trim((string) $value);
                return $trimmed === '' ? null : $trimmed;
            }, $row);
        }

        fclose($handle);

        return [
            'sheets' => [
                [
                    'name' => 'Sheet1',
                    'rows' => $rows,
                ],
            ],
        ];
    }

    private function detectDelimiter(string $line): string
    {
        $candidates = [',', ';', "\t", '|'];
        $bestDelimiter = ',';
        $bestCount = -1;

        foreach ($candidates as $candidate) {
            $count = substr_count($line, $candidate);
            if ($count > $bestCount) {
                $bestCount = $count;
                $bestDelimiter = $candidate;
            }
        }

        return $bestDelimiter;
    }
}
