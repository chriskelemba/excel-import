<?php

namespace ChrisKelemba\ExcelImport\Yii;

use yii\base\Application;
use yii\base\BootstrapInterface;

class Bootstrap implements BootstrapInterface
{
    public function bootstrap($app): void
    {
        if (!$app instanceof Application) {
            return;
        }

        if (!isset($app->controllerMap['imports'])) {
            $app->controllerMap['imports'] = ImportController::class;
        }
    }
}
