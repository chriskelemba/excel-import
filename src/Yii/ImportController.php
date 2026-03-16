<?php

namespace ChrisKelemba\ExcelImport\Yii;

use ChrisKelemba\ExcelImport\Core\Exceptions\ImportException;
use ChrisKelemba\ExcelImport\DynamicImporter;
use ChrisKelemba\ExcelImport\Http\ImportHttpActions;
use yii\web\Controller;
use yii\web\Response;

class ImportController extends Controller
{
    public $enableCsrfValidation = false;

    public function actionTemplate(): array
    {
        return $this->executeImportAction(fn (): array => $this->importActions()->template(\Yii::$app->request));
    }

    public function actionDatabases(): array
    {
        return $this->executeImportAction(fn (): array => $this->importActions()->databases(\Yii::$app->request));
    }

    public function actionRecords(): array
    {
        return $this->executeImportAction(fn (): array => $this->importActions()->records(\Yii::$app->request));
    }

    public function actionPreview(): array
    {
        return $this->executeImportAction(fn (): array => $this->importActions()->preview(\Yii::$app->request));
    }

    public function actionRun(): array
    {
        return $this->executeImportAction(fn (): array => $this->importActions()->run(\Yii::$app->request));
    }

    private function importActions(): ImportHttpActions
    {
        $db = \Yii::$app->get('db', false);
        if (!is_object($db)) {
            throw new ImportException('Yii `db` component is not configured.');
        }

        $pdo = null;
        if (method_exists($db, 'getPdo')) {
            try {
                $candidate = $db->getPdo();
                if ($candidate instanceof \PDO) {
                    $pdo = $candidate;
                }
            } catch (\Throwable $e) {
                throw new ImportException('Failed to open Yii DB connection: ' . $e->getMessage());
            }
        }

        if ($pdo === null && property_exists($db, 'pdo') && $db->pdo instanceof \PDO) {
            $pdo = $db->pdo;
        }

        if ($pdo === null && property_exists($db, 'dsn') && is_string($db->dsn) && trim($db->dsn) !== '') {
            $dsn = $db->dsn;
            $username = property_exists($db, 'username') ? (string) ($db->username ?? '') : '';
            $password = property_exists($db, 'password') ? (string) ($db->password ?? '') : '';
            $attributes = property_exists($db, 'attributes') && is_array($db->attributes)
                ? $db->attributes
                : [];

            try {
                $pdo = new \PDO($dsn, $username, $password, $attributes);
            } catch (\Throwable $e) {
                throw new ImportException('Failed to create PDO from Yii DB config: ' . $e->getMessage());
            }
        }

        if (!$pdo instanceof \PDO) {
            throw new ImportException('Yii `db` component did not provide a PDO connection.');
        }

        return (new DynamicImporter())
            ->addPdoConnection('yii', $pdo)
            ->withConfig(['connection' => 'yii'])
            ->http();
    }

    private function executeImportAction(callable $handler): array
    {
        \Yii::$app->response->format = Response::FORMAT_JSON;

        try {
            return $handler();
        } catch (ImportException $e) {
            \Yii::$app->response->statusCode = 422;
            return [
                'message' => 'Import failed.',
                'error' => $e->getMessage(),
            ];
        }
    }
}
