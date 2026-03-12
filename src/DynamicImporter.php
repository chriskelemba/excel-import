<?php

namespace ChrisKelemba\ExcelImport;

use ChrisKelemba\ExcelImport\Database\ConnectionManager;
use ChrisKelemba\ExcelImport\Database\MongoDatabaseAdapter;
use ChrisKelemba\ExcelImport\Database\PdoDatabaseAdapter;
use ChrisKelemba\ExcelImport\Registry\ImportTableRegistry;
use ChrisKelemba\ExcelImport\Service\DynamicImportService;
use PDO;

class DynamicImporter
{
    private ConnectionManager $connections;

    private array $config = [];

    public function __construct(?ConnectionManager $connections = null, array $config = [])
    {
        $this->connections = $connections ?? new ConnectionManager();
        $this->config = $config;
    }

    public function addPdoConnection(string $name, PDO $pdo): self
    {
        $this->connections->register($name, new PdoDatabaseAdapter($pdo));
        return $this;
    }

    public function addMongoConnection(string $name, object $client, string $database): self
    {
        $this->connections->register($name, new MongoDatabaseAdapter($client, $database));
        return $this;
    }

    public function addConnection(string $name, \ChrisKelemba\ExcelImport\Database\DatabaseAdapterInterface $adapter): self
    {
        $this->connections->register($name, $adapter);
        return $this;
    }

    public function withConfig(array $config): self
    {
        $clone = clone $this;
        $clone->config = $config;

        return $clone;
    }

    public function service(): DynamicImportService
    {
        $registry = new ImportTableRegistry($this->connections, $this->config);

        return new DynamicImportService($registry, config: $this->config);
    }
}
