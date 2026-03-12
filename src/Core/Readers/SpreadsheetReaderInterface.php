<?php

namespace ChrisKelemba\ExcelImport\Core\Readers;

interface SpreadsheetReaderInterface
{
    public function supports(string $extension): bool;

    /**
     * @return array{
     *   sheets: array<int, array{
     *     name: string,
     *     rows: array<int, array<int, string|null>>,
     *     grid?: array{
     *       column_labels: array<int, string>,
     *       rows: array<int, array{
     *         row_number: int,
     *         cells: array<int, array{
     *           value?: mixed,
     *           skip?: bool,
     *           rowspan?: int,
     *           colspan?: int,
     *           style?: array<string, mixed>
     *         }>
     *       }>
     *     }
     *   }>
     * }
     */
    public function read(string $path): array;
}
