<?php

namespace ChrisKelemba\ExcelImport\Http;

use ChrisKelemba\ExcelImport\Core\Exceptions\ImportException;
use ChrisKelemba\ExcelImport\Service\DynamicImportService;
use ChrisKelemba\ExcelImport\Workflow\ImportWorkflow;

class ImportHttpActions
{
    public function __construct(
        private readonly DynamicImportService $service,
        private readonly ImportWorkflow $workflow
    ) {
    }

    public function template(mixed $request = null): array
    {
        $input = $this->resolveInput($request);

        return $this->service->template(
            table: $this->stringValue($input, ['table']),
            connection: $this->stringValue($input, ['connection'])
        );
    }

    public function databases(mixed $request = null): array
    {
        $input = $this->resolveInput($request);

        return $this->service->databases(
            connection: $this->stringValue($input, ['connection']),
            table: $this->stringValue($input, ['table'])
        );
    }

    public function records(mixed $request = null): array
    {
        $input = $this->resolveInput($request);
        $table = trim((string) ($input['table'] ?? ''));
        if ($table === '') {
            throw new ImportException('`table` is required.');
        }

        return $this->service->records(
            table: $table,
            connection: $this->stringValue($input, ['connection']),
            page: $this->intValue($input, ['page']) ?? 1,
            perPage: $this->intValue($input, ['per_page', 'perPage']) ?? 25
        );
    }

    public function preview(mixed $request = null): array
    {
        $input = $this->resolveInput($request);
        [$filePath, $originalName] = $this->resolveFile($request, $input);
        $payload = $this->normalizeWorkflowPayload($input);

        return $this->workflow->previewFromPayload($filePath, $originalName, $payload);
    }

    public function run(mixed $request = null): array
    {
        $input = $this->resolveInput($request);
        [$filePath, $originalName] = $this->resolveFile($request, $input);
        $payload = $this->normalizeWorkflowPayload($input);

        return $this->workflow->runFromPayload($filePath, $originalName, $payload);
    }

    public function runMulti(mixed $request = null): array
    {
        return $this->run($request);
    }

    public function routes(): array
    {
        return [
            'template' => [$this, 'template'],
            'databases' => [$this, 'databases'],
            'records' => [$this, 'records'],
            'preview' => [$this, 'preview'],
            'run' => [$this, 'run'],
            'runMulti' => [$this, 'runMulti'],
        ];
    }

    private function normalizeWorkflowPayload(array $input): array
    {
        $payload = $input;

        if (isset($payload['imports']) && is_string($payload['imports'])) {
            $decoded = json_decode($payload['imports'], true);
            if (is_array($decoded)) {
                $payload['imports'] = $decoded;
            }
        }

        if (isset($payload['imports']) && is_array($payload['imports'])) {
            return $payload;
        }

        $table = trim((string) ($payload['table'] ?? ''));
        if ($table === '') {
            throw new ImportException('`imports` or `table` is required.');
        }

        $columnMap = $this->arrayValue($payload, ['column_map', 'columnMap']);

        $payload['imports'] = [[
            'table' => $table,
            'connection' => $this->stringValue($payload, ['connection']),
            'column_map' => $columnMap,
            'static_values' => $this->arrayValue($payload, ['static_values', 'staticValues']),
            'header_row' => $this->intValue($payload, ['header_row', 'headerRow']),
            'sample_rows' => $this->intValue($payload, ['sample_rows', 'sampleRows']) ?? 10,
            'mode' => $this->stringValue($payload, ['mode']),
            'unique_by' => $this->arrayValue($payload, ['unique_by', 'uniqueBy']),
            'sheet_index' => $this->intValue($payload, ['sheet_index', 'sheetIndex']) ?? 0,
        ]];

        return $payload;
    }

    private function resolveFile(mixed $request, array $input): array
    {
        $filePath = $this->stringValue($input, ['file_path', 'filePath']);
        $originalName = $this->stringValue($input, ['original_name', 'originalName']);

        if ($filePath !== null) {
            if (!is_file($filePath)) {
                throw new ImportException("`file_path` does not exist: {$filePath}");
            }

            return [$filePath, $originalName ?? basename($filePath)];
        }

        $resolvedUpload = $this->resolveUploadedFile($request, $input);
        if ($resolvedUpload !== null) {
            return $resolvedUpload;
        }

        throw new ImportException('Uploaded `file` is required.');
    }

    private function resolveUploadedFile(mixed $request, array $input): ?array
    {
        $fileKey = $this->stringValue($input, ['file_key', 'fileKey']) ?? 'file';

        if (is_object($request)) {
            $candidates = [];

            if (method_exists($request, 'file')) {
                $candidates[] = $this->safeCall($request, 'file', [$fileKey]);
            }

            if (property_exists($request, 'files') && is_object($request->files) && method_exists($request->files, 'get')) {
                $candidates[] = $this->safeCall($request->files, 'get', [$fileKey]);
            }

            if (method_exists($request, 'getUploadedFiles')) {
                $uploadedFiles = $this->safeCall($request, 'getUploadedFiles');
                if (is_array($uploadedFiles)) {
                    $candidates[] = $uploadedFiles[$fileKey] ?? null;
                }
            }

            if (method_exists($request, 'getFile')) {
                $candidates[] = $this->safeCall($request, 'getFile', [$fileKey]);
            }

            foreach ($candidates as $candidate) {
                $resolved = $this->normalizeUploadedFile($candidate);
                if ($resolved !== null) {
                    return $resolved;
                }
            }
        }

        if (isset($_FILES[$fileKey])) {
            return $this->normalizeUploadedFile($_FILES[$fileKey]);
        }

        return null;
    }

    private function normalizeUploadedFile(mixed $uploaded): ?array
    {
        if ($uploaded === null) {
            return null;
        }

        if (is_array($uploaded)) {
            $path = (string) ($uploaded['tmp_name'] ?? $uploaded['tmpName'] ?? $uploaded['tempName'] ?? '');
            $name = (string) ($uploaded['name'] ?? $uploaded['original_name'] ?? 'upload');
            if ($path !== '' && is_file($path)) {
                return [$path, $name !== '' ? $name : basename($path)];
            }

            return null;
        }

        if (!is_object($uploaded)) {
            return null;
        }

        $path = $this->safeStringCall($uploaded, ['getRealPath', 'getPathname']);
        if ($path === null && property_exists($uploaded, 'tempName')) {
            $path = is_string($uploaded->tempName) ? $uploaded->tempName : null;
        }

        $name = $this->safeStringCall($uploaded, ['getClientOriginalName', 'getClientFilename']);
        if ($name === null && property_exists($uploaded, 'name')) {
            $name = is_string($uploaded->name) ? $uploaded->name : null;
        }

        if ($path === null || !is_file($path)) {
            return null;
        }

        return [$path, $name ?? basename($path)];
    }

    private function resolveInput(mixed $request): array
    {
        if (is_array($request)) {
            return $request;
        }

        if (is_string($request)) {
            $decoded = json_decode($request, true);
            return is_array($decoded) ? $decoded : [];
        }

        $input = [];
        $input = array_replace($input, $_GET);
        $input = array_replace($input, $_POST);

        if (is_object($request)) {
            $body = $this->extractBody($request);
            if ($body !== []) {
                $input = array_replace($input, $body);
            }

            $query = $this->extractQuery($request);
            if ($query !== []) {
                $input = array_replace($input, $query);
            }
        }

        if ($input === []) {
            $raw = file_get_contents('php://input');
            if (is_string($raw) && trim($raw) !== '') {
                $decoded = json_decode($raw, true);
                if (is_array($decoded)) {
                    $input = $decoded;
                }
            }
        }

        return $input;
    }

    private function extractBody(object $request): array
    {
        $candidates = [];

        if (method_exists($request, 'all')) {
            $candidates[] = $this->safeCall($request, 'all');
        }

        if (method_exists($request, 'getBodyParams')) {
            $candidates[] = $this->safeCall($request, 'getBodyParams');
        }

        if (method_exists($request, 'getParsedBody')) {
            $candidates[] = $this->safeCall($request, 'getParsedBody');
        }

        if (method_exists($request, 'toArray')) {
            $candidates[] = $this->safeCall($request, 'toArray');
        }

        foreach ($candidates as $candidate) {
            if (is_array($candidate)) {
                return $candidate;
            }
        }

        return [];
    }

    private function extractQuery(object $request): array
    {
        $candidates = [];

        if (method_exists($request, 'query')) {
            $candidates[] = $this->safeCall($request, 'query');
        }

        if (method_exists($request, 'getQueryParams')) {
            $candidates[] = $this->safeCall($request, 'getQueryParams');
        }

        if (method_exists($request, 'get')) {
            $candidates[] = $this->safeCall($request, 'get');
        }

        foreach ($candidates as $candidate) {
            if (is_array($candidate)) {
                return $candidate;
            }
        }

        return [];
    }

    private function arrayValue(array $source, array $keys): array
    {
        foreach ($keys as $key) {
            if (array_key_exists($key, $source) && is_array($source[$key])) {
                return $source[$key];
            }
        }

        return [];
    }

    private function intValue(array $source, array $keys): ?int
    {
        foreach ($keys as $key) {
            if (!array_key_exists($key, $source) || $source[$key] === null || $source[$key] === '') {
                continue;
            }

            return (int) $source[$key];
        }

        return null;
    }

    private function stringValue(array $source, array $keys): ?string
    {
        foreach ($keys as $key) {
            if (!array_key_exists($key, $source) || $source[$key] === null) {
                continue;
            }

            $value = trim((string) $source[$key]);
            if ($value !== '') {
                return $value;
            }
        }

        return null;
    }

    private function safeCall(object $target, string $method, array $args = []): mixed
    {
        try {
            return $target->{$method}(...$args);
        } catch (\Throwable) {
            return null;
        }
    }

    private function safeStringCall(object $target, array $methods): ?string
    {
        foreach ($methods as $method) {
            if (!method_exists($target, $method)) {
                continue;
            }

            $value = $this->safeCall($target, $method);
            if (!is_string($value)) {
                continue;
            }

            $value = trim($value);
            if ($value !== '') {
                return $value;
            }
        }

        return null;
    }
}
