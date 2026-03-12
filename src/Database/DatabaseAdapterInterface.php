<?php

namespace ChrisKelemba\ExcelImport\Database;

interface DatabaseAdapterInterface
{
    public function driverName(): string;

    /** @return array<int, string> */
    public function listTables(): array;

    public function hasTable(string $table): bool;

    /** @return array<int, string> */
    public function listColumns(string $table, int $sampleSize = 50): array;

    /** @param array<int, string> $columns
     *  @return array<string, string|null>
     */
    public function listColumnTypes(string $table, array $columns): array;

    /** @return array{total:int,rows:array<int, array<string,mixed>>} */
    public function fetchRecords(string $table, int $offset, int $limit): array;

    /** @param array<string,mixed> $row */
    public function insertRow(string $table, array $row): void;

    /** @param array<string,mixed> $row
     *  @param array<int,string> $uniqueBy
     */
    public function upsertRow(string $table, array $row, array $uniqueBy): void;
}
