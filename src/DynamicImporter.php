<?php

namespace ChrisKelemba\ExcelImport;

use ChrisKelemba\ExcelImport\Database\ConnectionManager;
use ChrisKelemba\ExcelImport\Database\MongoDatabaseAdapter;
use ChrisKelemba\ExcelImport\Database\PdoDatabaseAdapter;
use ChrisKelemba\ExcelImport\Http\ImportHttpActions;
use ChrisKelemba\ExcelImport\Registry\ImportTableRegistry;
use ChrisKelemba\ExcelImport\Service\DynamicImportService;
use ChrisKelemba\ExcelImport\Workflow\ImportWorkflow;
use PDO;

class DynamicImporter
{
    private ConnectionManager $connections;

    private array $config = [];
    private bool $attemptedRuntimeBootstrap = false;

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
        $this->ensureRuntimeConnections();
        $registry = new ImportTableRegistry($this->connections, $this->config);

        return new DynamicImportService($registry, config: $this->config);
    }

    public function workflow(): ImportWorkflow
    {
        return new ImportWorkflow($this->service());
    }

    public function http(): ImportHttpActions
    {
        $service = $this->service();

        return new ImportHttpActions($service, new ImportWorkflow($service));
    }

    private function ensureRuntimeConnections(): void
    {
        if ($this->connections->names() !== []) {
            return;
        }

        if ($this->attemptedRuntimeBootstrap) {
            return;
        }
        $this->attemptedRuntimeBootstrap = true;

        $this->bootstrapLaravelConnection();
    }

    private function bootstrapLaravelConnection(): void
    {
        if (!function_exists('app')) {
            return;
        }

        try {
            $dbManager = app('db');
        } catch (\Throwable) {
            return;
        }

        if (!is_object($dbManager) || !method_exists($dbManager, 'connection')) {
            return;
        }

        $configuredConnection = trim((string) ($this->config['connection'] ?? ''));
        $connectionName = $configuredConnection;

        if ($connectionName === '' && function_exists('config')) {
            try {
                $connectionName = trim((string) config('database.default', ''));
            } catch (\Throwable) {
                $connectionName = '';
            }
        }

        try {
            $connection = $connectionName !== ''
                ? $dbManager->connection($connectionName)
                : $dbManager->connection();
        } catch (\Throwable) {
            return;
        }

        if (!is_object($connection) || !method_exists($connection, 'getPdo')) {
            return;
        }

        try {
            $pdo = $connection->getPdo();
        } catch (\Throwable) {
            return;
        }

        if (!$pdo instanceof PDO) {
            return;
        }

        $resolvedName = $connectionName;
        if ($resolvedName === '' && method_exists($connection, 'getName')) {
            try {
                $resolvedName = trim((string) $connection->getName());
            } catch (\Throwable) {
                $resolvedName = '';
            }
        }

        $this->addPdoConnection($resolvedName !== '' ? $resolvedName : 'default', $pdo);
    }
}
