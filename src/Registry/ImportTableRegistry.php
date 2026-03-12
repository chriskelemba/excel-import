<?php

namespace ChrisKelemba\ExcelImport\Registry;

use ChrisKelemba\ExcelImport\Config\Defaults;
use ChrisKelemba\ExcelImport\Core\Exceptions\ImportException;
use ChrisKelemba\ExcelImport\Database\ConnectionManager;

class ImportTableRegistry
{
    private array $config;

    public function __construct(
        private readonly ConnectionManager $connections,
        array $config = []
    ) {
        $this->config = $this->mergeConfig(Defaults::values(), $config);
    }

    public function all(?string $connection = null): array
    {
        $tables = $this->discoverImportableTables($connection);
        $resolved = [];

        foreach ($tables as $table) {
            $resolved[$table] = $this->definition($table, $connection);
        }

        return $resolved;
    }

    public function has(string $table, ?string $connection = null): bool
    {
        $table = trim($table);
        if ($table === '') {
            return false;
        }

        $excluded = array_map('strtolower', (array) $this->getConfig('discovery.exclude_tables', []));
        if (in_array(strtolower($table), $excluded, true)) {
            return false;
        }

        $allowTables = array_values(array_filter((array) $this->getConfig('discovery.allow_tables', []), 'is_string'));
        if ($allowTables !== [] && !in_array($table, $allowTables, true)) {
            return false;
        }

        $resolvedConnection = $this->resolveConnection($connection);
        $adapter = $this->connections->get($resolvedConnection);

        if (!$adapter->hasTable($table) && !array_key_exists($table, $this->tableOverrides())) {
            return false;
        }

        if ((bool) $this->getConfig('discovery.allow_unconfigured_tables', true)) {
            return true;
        }

        return array_key_exists($table, $this->tableOverrides());
    }

    public function definition(string $table, ?string $connection = null): array
    {
        $entry = (array) ($this->tableOverrides()[$table] ?? []);
        $resolvedConnection = $this->resolveConnection($connection);
        $adapter = $this->connections->get($resolvedConnection);

        $columns = array_values(array_unique(array_filter(
            $entry['columns'] ?? $this->discoverColumns($table, $resolvedConnection),
            static fn ($value): bool => is_string($value) && trim($value) !== ''
        )));

        $required = array_values(array_filter((array) ($entry['required'] ?? []), static function ($column) use ($columns): bool {
            return is_string($column) && in_array($column, $columns, true);
        }));

        $uniqueBy = array_values(array_filter((array) ($entry['unique_by'] ?? []), static function ($column) use ($columns): bool {
            return is_string($column) && in_array($column, $columns, true);
        }));

        $mode = in_array(($entry['mode'] ?? 'insert'), ['insert', 'upsert'], true)
            ? ($entry['mode'] ?? 'insert')
            : 'insert';

        return [
            'table' => $table,
            'connection' => $resolvedConnection,
            'driver' => $adapter->driverName(),
            'columns' => $columns,
            'column_types' => $this->discoverColumnTypes($table, $columns, $resolvedConnection, $entry),
            'required' => $required,
            'unique_by' => $uniqueBy,
            'mode' => $mode,
            'static_values' => $this->resolveStaticValues($entry, $columns),
        ];
    }

    public function resolveConnection(?string $connection = null): string
    {
        $resolved = is_string($connection) ? trim($connection) : '';
        if ($resolved !== '') {
            if (!$this->connections->has($resolved)) {
                throw new ImportException("Connection '{$resolved}' is not registered.");
            }

            return $resolved;
        }

        $fromConfig = trim((string) $this->getConfig('connection', ''));
        if ($fromConfig !== '') {
            if (!$this->connections->has($fromConfig)) {
                throw new ImportException("Configured connection '{$fromConfig}' is not registered.");
            }

            return $fromConfig;
        }

        $names = $this->connections->names();
        if ($names === []) {
            throw new ImportException('No database connections are registered.');
        }

        return $names[0];
    }

    public function connections(): ConnectionManager
    {
        return $this->connections;
    }

    /** @return array<int,string> */
    private function discoverImportableTables(?string $connection = null): array
    {
        $resolvedConnection = $this->resolveConnection($connection);
        $overrides = $this->tableOverrides();
        $tables = $this->connections->get($resolvedConnection)->listTables();

        if ($this->connections->get($resolvedConnection)->driverName() === 'mongodb') {
            $tables = array_values(array_unique(array_merge($tables, array_keys($overrides))));
        }

        $excluded = array_map('strtolower', (array) $this->getConfig('discovery.exclude_tables', []));
        $allowTables = array_values(array_filter((array) $this->getConfig('discovery.allow_tables', []), 'is_string'));
        $allowUnconfigured = (bool) $this->getConfig('discovery.allow_unconfigured_tables', true);

        $importable = [];
        foreach ($tables as $table) {
            if (in_array(strtolower($table), $excluded, true)) {
                continue;
            }

            if ($allowTables !== [] && !in_array($table, $allowTables, true)) {
                continue;
            }

            if (!$allowUnconfigured && !array_key_exists($table, $overrides)) {
                continue;
            }

            $importable[] = $table;
        }

        sort($importable);

        return $importable;
    }

    /** @return array<string,mixed> */
    private function tableOverrides(): array
    {
        return (array) $this->getConfig('tables', []);
    }

    /** @return array<int,string> */
    private function discoverColumns(string $table, string $connection): array
    {
        $sampleLimit = max(1, (int) $this->getConfig('mongodb.column_discovery.sample_documents', 50));
        return $this->connections->get($connection)->listColumns($table, $sampleLimit);
    }

    /** @param array<int,string> $columns
     *  @param array<string,mixed> $entry
     *  @return array<string,string|null>
     */
    private function discoverColumnTypes(string $table, array $columns, string $connection, array $entry = []): array
    {
        if (isset($entry['column_types']) && is_array($entry['column_types'])) {
            return $entry['column_types'];
        }

        return $this->connections->get($connection)->listColumnTypes($table, $columns);
    }

    /** @param array<string,mixed> $entry
     *  @param array<int,string> $columns
     *  @return array<string,mixed>
     */
    private function resolveStaticValues(array $entry, array $columns): array
    {
        $resolved = [];
        foreach ((array) ($entry['static_values'] ?? []) as $column => $value) {
            if (!is_string($column) || trim($column) === '') {
                continue;
            }

            if (!in_array($column, $columns, true)) {
                continue;
            }

            $resolved[$column] = $value;
        }

        return $resolved;
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
}
