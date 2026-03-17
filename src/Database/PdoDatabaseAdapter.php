<?php

namespace ChrisKelemba\ExcelImport\Database;

use ChrisKelemba\ExcelImport\Core\Exceptions\ImportException;
use PDO;
use PDOStatement;

class PdoDatabaseAdapter implements DatabaseAdapterInterface
{
    public function __construct(
        private readonly PDO $pdo
    ) {}

    public function driverName(): string
    {
        return strtolower((string) $this->pdo->getAttribute(PDO::ATTR_DRIVER_NAME));
    }

    public function listTables(): array
    {
        try {
            $rows = match ($this->driverName()) {
                'mysql', 'mariadb' => $this->pdo->query('SHOW TABLES')?->fetchAll(PDO::FETCH_NUM) ?? [],
                'pgsql' => $this->pdo->query("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname NOT IN ('pg_catalog', 'information_schema')")?->fetchAll(PDO::FETCH_NUM) ?? [],
                'sqlite' => $this->pdo->query("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'")?->fetchAll(PDO::FETCH_NUM) ?? [],
                'sqlsrv' => $this->pdo->query("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")?->fetchAll(PDO::FETCH_NUM) ?? [],
                default => [],
            };
        } catch (\Throwable) {
            return [];
        }

        $tables = [];
        foreach ($rows as $row) {
            $value = $row[0] ?? null;
            if (is_string($value) && trim($value) !== '') {
                $tables[] = $value;
            }
        }

        return array_values(array_unique($tables));
    }

    public function hasTable(string $table): bool
    {
        return in_array($table, $this->listTables(), true);
    }

    public function listColumns(string $table, int $sampleSize = 50): array
    {
        return array_keys($this->listColumnTypes($table, []));
    }

    public function listColumnTypes(string $table, array $columns): array
    {
        try {
            return match ($this->driverName()) {
                'mysql', 'mariadb' => $this->mysqlColumnTypes($table),
                'pgsql' => $this->pgsqlColumnTypes($table),
                'sqlite' => $this->sqliteColumnTypes($table),
                'sqlsrv' => $this->sqlsrvColumnTypes($table),
                default => array_fill_keys($columns, null),
            };
        } catch (\Throwable) {
            return array_fill_keys($columns, null);
        }
    }

    public function fetchRecords(string $table, int $offset, int $limit): array
    {
        $offset = max(0, $offset);
        $limit = max(1, $limit);

        $countSql = 'SELECT COUNT(*) FROM ' . $this->quoteIdentifier($table);
        $total = (int) $this->pdo->query($countSql)->fetchColumn();

        $sql = match ($this->driverName()) {
            'sqlsrv' => 'SELECT * FROM ' . $this->quoteIdentifier($table) . ' ORDER BY (SELECT NULL) OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY',
            default => 'SELECT * FROM ' . $this->quoteIdentifier($table) . ' LIMIT :limit OFFSET :offset',
        };

        $stmt = $this->pdo->prepare($sql);
        $stmt->bindValue(':limit', $limit, PDO::PARAM_INT);
        $stmt->bindValue(':offset', $offset, PDO::PARAM_INT);
        $stmt->execute();

        return [
            'total' => $total,
            'rows' => $stmt->fetchAll(PDO::FETCH_ASSOC) ?: [],
        ];
    }

    public function insertRow(string $table, array $row): void
    {
        if ($row === []) {
            throw new ImportException('Cannot insert an empty row.');
        }

        $columns = array_keys($row);
        $columnSql = implode(', ', array_map($this->quoteIdentifier(...), $columns));
        $placeholders = implode(', ', array_map(static fn (string $column): string => ':' . $column, $columns));

        $sql = 'INSERT INTO ' . $this->quoteIdentifier($table) . " ({$columnSql}) VALUES ({$placeholders})";
        $stmt = $this->pdo->prepare($sql);
        $this->bindRowValues($stmt, $row);
        $stmt->execute();
    }

    public function upsertRow(string $table, array $row, array $uniqueBy): void
    {
        if ($uniqueBy === []) {
            throw new ImportException("Import mode 'upsert' requires unique_by columns.");
        }

        foreach ($uniqueBy as $column) {
            if (!array_key_exists($column, $row)) {
                throw new ImportException("Missing unique_by column '{$column}' in mapped row.");
            }
        }

        $driver = $this->driverName();
        try {
            match ($driver) {
                'mysql', 'mariadb' => $this->upsertRowMysql($table, $row),
                'pgsql', 'sqlite' => $this->upsertRowWithConflict($table, $row, $uniqueBy),
                'sqlsrv' => $this->upsertRowSqlsrvMerge($table, $row, $uniqueBy),
                default => $this->upsertRowLegacy($table, $row, $uniqueBy),
            };
        } catch (\Throwable $e) {
            if ($this->shouldFallbackToLegacyUpsert($driver, $e->getMessage())) {
                $this->upsertRowLegacy($table, $row, $uniqueBy);
                return;
            }

            throw $e;
        }
    }

    private function upsertRowMysql(string $table, array $row): void
    {
        $columns = array_keys($row);
        $columnSql = implode(', ', array_map($this->quoteIdentifier(...), $columns));
        $placeholders = implode(', ', array_fill(0, count($columns), '?'));
        $updateSql = implode(', ', array_map(function (string $column): string {
            $quoted = $this->quoteIdentifier($column);
            return $quoted . ' = VALUES(' . $quoted . ')';
        }, $columns));

        $sql = 'INSERT INTO ' . $this->quoteIdentifier($table)
            . " ({$columnSql}) VALUES ({$placeholders}) ON DUPLICATE KEY UPDATE {$updateSql}";

        $stmt = $this->pdo->prepare($sql);
        $stmt->execute(array_values($row));
    }

    private function upsertRowWithConflict(string $table, array $row, array $uniqueBy): void
    {
        $columns = array_keys($row);
        $columnSql = implode(', ', array_map($this->quoteIdentifier(...), $columns));
        $placeholders = implode(', ', array_fill(0, count($columns), '?'));
        $conflictSql = implode(', ', array_map($this->quoteIdentifier(...), $uniqueBy));
        $updateSql = implode(', ', array_map(function (string $column): string {
            $quoted = $this->quoteIdentifier($column);
            return $quoted . ' = EXCLUDED.' . $quoted;
        }, $columns));

        $sql = 'INSERT INTO ' . $this->quoteIdentifier($table)
            . " ({$columnSql}) VALUES ({$placeholders})"
            . " ON CONFLICT ({$conflictSql}) DO UPDATE SET {$updateSql}";

        $stmt = $this->pdo->prepare($sql);
        $stmt->execute(array_values($row));
    }

    private function upsertRowSqlsrvMerge(string $table, array $row, array $uniqueBy): void
    {
        $columns = array_keys($row);
        $sourceSelect = implode(', ', array_map(function (string $column): string {
            return '? AS ' . $this->quoteIdentifier($column);
        }, $columns));

        $on = implode(' AND ', array_map(function (string $column): string {
            $quoted = $this->quoteIdentifier($column);
            return 'target.' . $quoted . ' = source.' . $quoted;
        }, $uniqueBy));

        $updateSet = implode(', ', array_map(function (string $column): string {
            $quoted = $this->quoteIdentifier($column);
            return 'target.' . $quoted . ' = source.' . $quoted;
        }, $columns));

        $insertColumns = implode(', ', array_map($this->quoteIdentifier(...), $columns));
        $insertValues = implode(', ', array_map(function (string $column): string {
            return 'source.' . $this->quoteIdentifier($column);
        }, $columns));

        $sql = 'MERGE ' . $this->quoteIdentifier($table) . ' WITH (HOLDLOCK) AS target '
            . "USING (SELECT {$sourceSelect}) AS source "
            . "ON {$on} "
            . "WHEN MATCHED THEN UPDATE SET {$updateSet} "
            . "WHEN NOT MATCHED THEN INSERT ({$insertColumns}) VALUES ({$insertValues});";

        $stmt = $this->pdo->prepare($sql);
        $stmt->execute(array_values($row));
    }

    private function upsertRowLegacy(string $table, array $row, array $uniqueBy): void
    {
        $where = [];
        $params = [];
        foreach ($uniqueBy as $column) {
            $where[] = $this->quoteIdentifier($column) . ' = :' . $column;
            $params[$column] = $row[$column];
        }

        $existsSql = 'SELECT COUNT(*) FROM ' . $this->quoteIdentifier($table)
            . ' WHERE ' . implode(' AND ', $where);

        $existsStmt = $this->pdo->prepare($existsSql);
        $this->bindRowValues($existsStmt, $params);
        $existsStmt->execute();

        $exists = (int) $existsStmt->fetchColumn() > 0;

        if ($exists) {
            $set = [];
            foreach ($row as $column => $_value) {
                $set[] = $this->quoteIdentifier($column) . ' = :upd_' . $column;
            }

            $updateSql = 'UPDATE ' . $this->quoteIdentifier($table)
                . ' SET ' . implode(', ', $set)
                . ' WHERE ' . implode(' AND ', $where);

            $updateStmt = $this->pdo->prepare($updateSql);
            foreach ($row as $column => $value) {
                $updateStmt->bindValue(':upd_' . $column, $value);
            }
            $this->bindRowValues($updateStmt, $params);
            $updateStmt->execute();

            return;
        }

        $this->insertRow($table, $row);
    }

    private function shouldFallbackToLegacyUpsert(string $driver, string $message): bool
    {
        $message = strtolower($message);

        if (in_array($driver, ['pgsql', 'sqlite'], true)) {
            if (str_contains($message, 'no unique') || str_contains($message, 'no exclusion')) {
                return true;
            }

            if (str_contains($message, 'on conflict clause does not match')) {
                return true;
            }
        }

        return false;
    }

    /** @return array<string, string|null> */
    private function mysqlColumnTypes(string $table): array
    {
        $sql = 'SHOW COLUMNS FROM ' . $this->quoteIdentifier($table);
        $rows = $this->pdo->query($sql)?->fetchAll(PDO::FETCH_ASSOC) ?: [];
        $types = [];
        foreach ($rows as $row) {
            $field = (string) ($row['Field'] ?? '');
            if ($field === '') {
                continue;
            }

            $rawType = strtolower((string) ($row['Type'] ?? ''));
            $types[$field] = preg_replace('/\(.*/', '', $rawType) ?: null;
        }

        return $types;
    }

    /** @return array<string, string|null> */
    private function pgsqlColumnTypes(string $table): array
    {
        $sql = 'SELECT column_name, data_type FROM information_schema.columns WHERE table_name = :table ORDER BY ordinal_position';
        $stmt = $this->pdo->prepare($sql);
        $stmt->bindValue(':table', $table);
        $stmt->execute();

        $types = [];
        foreach ($stmt->fetchAll(PDO::FETCH_ASSOC) ?: [] as $row) {
            $column = (string) ($row['column_name'] ?? '');
            if ($column === '') {
                continue;
            }
            $types[$column] = strtolower((string) ($row['data_type'] ?? '')) ?: null;
        }

        return $types;
    }

    /** @return array<string, string|null> */
    private function sqliteColumnTypes(string $table): array
    {
        $sql = 'PRAGMA table_info(' . $this->quoteLiteral($table) . ')';
        $rows = $this->pdo->query($sql)?->fetchAll(PDO::FETCH_ASSOC) ?: [];
        $types = [];

        foreach ($rows as $row) {
            $name = (string) ($row['name'] ?? '');
            if ($name === '') {
                continue;
            }

            $rawType = strtolower((string) ($row['type'] ?? ''));
            $types[$name] = preg_replace('/\(.*/', '', $rawType) ?: null;
        }

        return $types;
    }

    /** @return array<string, string|null> */
    private function sqlsrvColumnTypes(string $table): array
    {
        $sql = 'SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = :table ORDER BY ORDINAL_POSITION';
        $stmt = $this->pdo->prepare($sql);
        $stmt->bindValue(':table', $table);
        $stmt->execute();

        $types = [];
        foreach ($stmt->fetchAll(PDO::FETCH_ASSOC) ?: [] as $row) {
            $column = (string) ($row['COLUMN_NAME'] ?? '');
            if ($column === '') {
                continue;
            }
            $types[$column] = strtolower((string) ($row['DATA_TYPE'] ?? '')) ?: null;
        }

        return $types;
    }

    private function quoteIdentifier(string $identifier): string
    {
        $parts = array_map('trim', explode('.', $identifier));
        $driver = $this->driverName();

        $wrapped = array_map(static function (string $part) use ($driver): string {
            if ($driver === 'mysql' || $driver === 'mariadb') {
                return '`' . str_replace('`', '``', $part) . '`';
            }

            if ($driver === 'sqlsrv') {
                return '[' . str_replace(']', ']]', $part) . ']';
            }

            return '"' . str_replace('"', '""', $part) . '"';
        }, $parts);

        return implode('.', $wrapped);
    }

    private function quoteLiteral(string $value): string
    {
        return $this->pdo->quote($value);
    }

    /** @param array<string,mixed> $row */
    private function bindRowValues(PDOStatement $stmt, array $row): void
    {
        foreach ($row as $column => $value) {
            $stmt->bindValue(':' . $column, $value);
        }
    }
}
