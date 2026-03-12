<?php

namespace ChrisKelemba\ExcelImport\Config;

class Defaults
{
    public static function values(): array
    {
        return [
            'connection' => null,
            'preview' => [
                'max_rows' => 200,
                'default_sample_rows' => 10,
            ],
            'discovery' => [
                'allow_unconfigured_tables' => true,
                'allow_tables' => [],
                'exclude_tables' => [
                    'migrations',
                    'failed_jobs',
                    'jobs',
                    'job_batches',
                    'password_reset_tokens',
                    'personal_access_tokens',
                ],
            ],
            'mongodb' => [
                'column_discovery' => [
                    'sample_documents' => 50,
                ],
            ],
            'tables' => [],
        ];
    }
}
