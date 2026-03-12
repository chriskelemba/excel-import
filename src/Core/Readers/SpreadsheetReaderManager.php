<?php

namespace ChrisKelemba\ExcelImport\Core\Readers;

use ChrisKelemba\ExcelImport\Core\Exceptions\ImportException;

class SpreadsheetReaderManager
{
    /** @var array<int, \ChrisKelemba\ExcelImport\Core\Readers\SpreadsheetReaderInterface> */
    private array $readers;

    public function __construct()
    {
        $this->readers = [
            new JsonSpreadsheetReader(),
            new CsvSpreadsheetReader(),
            new XlsxSpreadsheetReader(),
        ];
    }

    public function read(string $path, string $originalName): array
    {
        $extension = strtolower((string) pathinfo($originalName, PATHINFO_EXTENSION));

        foreach ($this->readers as $reader) {
            if ($reader->supports($extension)) {
                return $reader->read($path);
            }
        }

        throw new ImportException("Unsupported file format '{$extension}'.");
    }
}
