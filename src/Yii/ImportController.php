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
        return $this->executeImportAction(fn (): array => $this->actions()->template(\Yii::$app->request));
    }

    public function actionDatabases(): array
    {
        return $this->executeImportAction(fn (): array => $this->actions()->databases(\Yii::$app->request));
    }

    public function actionRecords(): array
    {
        return $this->executeImportAction(fn (): array => $this->actions()->records(\Yii::$app->request));
    }

    public function actionPreview(): array
    {
        return $this->executeImportAction(fn (): array => $this->actions()->preview(\Yii::$app->request));
    }

    public function actionRun(): array
    {
        return $this->executeImportAction(fn (): array => $this->actions()->run(\Yii::$app->request));
    }

    private function actions(): ImportHttpActions
    {
        $importer = new DynamicImporter();

        $db = \Yii::$app->get('db', false);
        if (is_object($db) && property_exists($db, 'pdo') && $db->pdo instanceof \PDO) {
            $importer = $importer
                ->addPdoConnection('yii', $db->pdo)
                ->withConfig(['connection' => 'yii']);
        }

        return $importer->http();
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
