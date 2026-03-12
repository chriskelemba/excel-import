<?php

namespace ChrisKelemba\ExcelImport\Database;

use ChrisKelemba\ExcelImport\Core\Exceptions\ImportException;

class MongoDatabaseAdapter implements DatabaseAdapterInterface
{
    public function __construct(
        private readonly object $client,
        private readonly string $databaseName
    ) {
        if (!class_exists('MongoDB\\Client')) {
            throw new ImportException('MongoDB support requires mongodb/mongodb.');
        }

        if (!is_a($client, 'MongoDB\\Client')) {
            throw new ImportException('MongoDatabaseAdapter expects an instance of MongoDB\\Client.');
        }
    }

    public function driverName(): string
    {
        return 'mongodb';
    }

    public function listTables(): array
    {
        $collections = [];

        try {
            foreach ($this->database()->listCollections() as $collectionInfo) {
                $name = method_exists($collectionInfo, 'getName') ? $collectionInfo->getName() : null;
                if (is_string($name) && trim($name) !== '') {
                    $collections[] = $name;
                }
            }
        } catch (\Throwable) {
            return [];
        }

        return array_values(array_unique($collections));
    }

    public function hasTable(string $table): bool
    {
        return in_array($table, $this->listTables(), true);
    }

    public function listColumns(string $table, int $sampleSize = 50): array
    {
        $sampleSize = max(1, $sampleSize);

        try {
            $cursor = $this->database()->selectCollection($table)->find([], ['limit' => $sampleSize]);
        } catch (\Throwable) {
            return [];
        }

        $columns = [];
        foreach ($cursor as $row) {
            $data = $this->rowToArray($row);
            unset($data['_id']);

            foreach (array_keys($data) as $key) {
                if (is_string($key) && trim($key) !== '') {
                    $columns[$key] = true;
                }
            }
        }

        return array_keys($columns);
    }

    public function listColumnTypes(string $table, array $columns): array
    {
        return array_fill_keys($columns, null);
    }

    public function fetchRecords(string $table, int $offset, int $limit): array
    {
        $offset = max(0, $offset);
        $limit = max(1, $limit);

        $collection = $this->database()->selectCollection($table);
        $total = (int) $collection->countDocuments();

        $rows = [];
        $cursor = $collection->find([], ['skip' => $offset, 'limit' => $limit]);
        foreach ($cursor as $row) {
            $rows[] = $this->rowToArray($row);
        }

        return ['total' => $total, 'rows' => $rows];
    }

    public function insertRow(string $table, array $row): void
    {
        $this->database()->selectCollection($table)->insertOne($row);
    }

    public function upsertRow(string $table, array $row, array $uniqueBy): void
    {
        if ($uniqueBy === []) {
            throw new ImportException("Import mode 'upsert' requires unique_by columns.");
        }

        $filter = [];
        foreach ($uniqueBy as $column) {
            if (!array_key_exists($column, $row)) {
                throw new ImportException("Missing unique_by column '{$column}' in mapped row.");
            }
            $filter[$column] = $row[$column];
        }

        $this->database()->selectCollection($table)->updateOne(
            $filter,
            ['$set' => $row],
            ['upsert' => true]
        );
    }

    private function database(): object
    {
        return $this->client->selectDatabase($this->databaseName);
    }

    /** @return array<string,mixed> */
    private function rowToArray(mixed $row): array
    {
        if (is_array($row)) {
            return $row;
        }

        if (is_object($row) && method_exists($row, 'getArrayCopy')) {
            try {
                $copy = $row->getArrayCopy();
                if (is_array($copy)) {
                    return $copy;
                }
            } catch (\Throwable) {
            }
        }

        if (is_object($row) && method_exists($row, 'jsonSerialize')) {
            try {
                $serialized = $row->jsonSerialize();
                if (is_array($serialized)) {
                    return $serialized;
                }
            } catch (\Throwable) {
            }
        }

        if (is_object($row)) {
            return get_object_vars($row);
        }

        return [];
    }
}
