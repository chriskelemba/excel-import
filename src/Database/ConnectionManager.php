<?php

namespace ChrisKelemba\ExcelImport\Database;

use ChrisKelemba\ExcelImport\Core\Exceptions\ImportException;

class ConnectionManager
{
    /** @var array<string, DatabaseAdapterInterface> */
    private array $adapters = [];

    public function register(string $name, DatabaseAdapterInterface $adapter): void
    {
        $name = trim($name);
        if ($name === '') {
            throw new ImportException('Connection name cannot be empty.');
        }

        $this->adapters[$name] = $adapter;
    }

    public function has(string $name): bool
    {
        return isset($this->adapters[$name]);
    }

    public function get(string $name): DatabaseAdapterInterface
    {
        if (!$this->has($name)) {
            throw new ImportException("Connection '{$name}' is not registered.");
        }

        return $this->adapters[$name];
    }

    /** @return array<string, array{driver:string}> */
    public function catalog(): array
    {
        $catalog = [];
        foreach ($this->adapters as $name => $adapter) {
            $catalog[$name] = ['driver' => $adapter->driverName()];
        }

        ksort($catalog);

        return $catalog;
    }

    /** @return array<int,string> */
    public function names(): array
    {
        return array_keys($this->adapters);
    }
}
