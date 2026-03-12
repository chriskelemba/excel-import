<?php

namespace ChrisKelemba\ExcelImport\Core\Readers;

use ChrisKelemba\ExcelImport\Core\Exceptions\ImportException;
use ChrisKelemba\ExcelImport\Core\Readers\SpreadsheetReaderInterface;

class XlsxSpreadsheetReader implements SpreadsheetReaderInterface
{
    public function supports(string $extension): bool
    {
        return strtolower($extension) === 'xlsx';
    }

    public function read(string $path): array
    {
        $ioFactoryClass = '\\PhpOffice\\PhpSpreadsheet\\IOFactory';
        if (!class_exists($ioFactoryClass)) {
            throw new ImportException('XLSX support requires phpoffice/phpspreadsheet.');
        }

        try {
            $spreadsheet = $ioFactoryClass::load($path);
        } catch (\Throwable $e) {
            throw new ImportException('Failed to read XLSX file: ' . $e->getMessage(), 0, $e);
        }

        $sheets = [];
        foreach ($spreadsheet->getWorksheetIterator() as $worksheet) {
            $rawRows = $worksheet->toArray(null, true, true, false);
            $rows = array_map(static function (array $row): array {
                return array_map(static function ($value) {
                    if ($value === null) {
                        return null;
                    }
                    $trimmed = trim((string) $value);
                    return $trimmed === '' ? null : $trimmed;
                }, $row);
            }, $rawRows);

            $sheets[] = [
                'name' => (string) $worksheet->getTitle(),
                'rows' => $rows,
                'grid' => $this->buildStyledGrid($worksheet),
            ];
        }

        return ['sheets' => $sheets];
    }

    private function buildStyledGrid(object $worksheet): array
    {
        $highestRow = max(1, (int) $worksheet->getHighestRow());
        $highestColumnIndex = max(1, $this->columnIndexFromString((string) $worksheet->getHighestColumn()));
        $mergeRanges = $worksheet->getMergeCells();
        $mergeMap = $this->buildMergeMap($mergeRanges);
        [$mergeMaxRow, $mergeMaxCol] = $this->maxMergeBounds($mergeRanges);
        $highestRow = max($highestRow, $mergeMaxRow);
        $highestColumnIndex = max($highestColumnIndex, $mergeMaxCol);

        $columnLabels = [];
        for ($col = 1; $col <= $highestColumnIndex; $col++) {
            $columnLabels[] = $this->stringFromColumnIndex($col);
        }

        $rows = [];
        for ($row = 1; $row <= $highestRow; $row++) {
            $cells = [];

            for ($col = 1; $col <= $highestColumnIndex; $col++) {
                $cellKey = $row . ':' . $col;
                if (isset($mergeMap[$cellKey]) && $mergeMap[$cellKey]['skip'] === true) {
                    $cells[] = ['skip' => true];
                    continue;
                }

                $coordinate = $this->stringFromColumnIndex($col) . $row;
                $cell = $worksheet->getCell($coordinate);
                $value = $cell->getFormattedValue();
                $value = is_string($value) ? trim($value) : $value;
                $style = $this->extractCellStyle($worksheet, $coordinate);

                $payload = ['value' => $value === '' ? null : $value];
                if (!empty($style)) {
                    $payload['style'] = $style;
                }

                if (isset($mergeMap[$cellKey]) && $mergeMap[$cellKey]['skip'] === false) {
                    $rowspan = (int) ($mergeMap[$cellKey]['rowspan'] ?? 1);
                    $colspan = (int) ($mergeMap[$cellKey]['colspan'] ?? 1);
                    if ($rowspan > 1) {
                        $payload['rowspan'] = $rowspan;
                    }
                    if ($colspan > 1) {
                        $payload['colspan'] = $colspan;
                    }
                }

                $cells[] = $payload;
            }

            $rows[] = [
                'row_number' => $row,
                'cells' => $cells,
            ];
        }

        return [
            'column_labels' => $columnLabels,
            'rows' => $rows,
        ];
    }

    /**
     * @param array<int, string> $mergeRanges
     * @return array{int, int}
     */
    private function maxMergeBounds(array $mergeRanges): array
    {
        $maxRow = 1;
        $maxCol = 1;

        foreach ($mergeRanges as $range) {
            $bounds = $this->rangeBoundaries((string) $range);
            $maxCol = max($maxCol, (int) ($bounds[1][0] ?? 1));
            $maxRow = max($maxRow, (int) ($bounds[1][1] ?? 1));
        }

        return [$maxRow, $maxCol];
    }

    /**
     * @param array<int, string> $mergeRanges
     * @return array<string, array{skip: bool, rowspan?: int, colspan?: int}>
     */
    private function buildMergeMap(array $mergeRanges): array
    {
        $map = [];

        foreach ($mergeRanges as $range) {
            $bounds = $this->rangeBoundaries((string) $range);
            $startCol = (int) ($bounds[0][0] ?? 1);
            $startRow = (int) ($bounds[0][1] ?? 1);
            $endCol = (int) ($bounds[1][0] ?? $startCol);
            $endRow = (int) ($bounds[1][1] ?? $startRow);

            for ($row = $startRow; $row <= $endRow; $row++) {
                for ($col = $startCol; $col <= $endCol; $col++) {
                    $key = $row . ':' . $col;
                    if ($row === $startRow && $col === $startCol) {
                        $map[$key] = [
                            'skip' => false,
                            'rowspan' => max(1, $endRow - $startRow + 1),
                            'colspan' => max(1, $endCol - $startCol + 1),
                        ];
                        continue;
                    }

                    $map[$key] = ['skip' => true];
                }
            }
        }

        return $map;
    }

    private function extractCellStyle(object $worksheet, string $coordinate): array
    {
        $style = $worksheet->getStyle($coordinate);
        $font = $style->getFont();
        $fill = $style->getFill();
        $alignment = $style->getAlignment();
        $borders = $style->getBorders();
        $payload = [];

        if ($font->getBold()) {
            $payload['bold'] = true;
        }
        if ($font->getItalic()) {
            $payload['italic'] = true;
        }
        if ($font->getUnderline() && $font->getUnderline() !== 'none') {
            $payload['underline'] = true;
        }

        $fontColor = $this->argbToHex($font->getColor()->getARGB());
        if ($fontColor !== null) {
            $payload['color'] = $fontColor;
        }

        $fontName = trim((string) $font->getName());
        if ($fontName !== '') {
            $payload['font_family'] = $fontName;
        }

        $fontSize = (float) $font->getSize();
        if ($fontSize > 0) {
            $payload['font_size_pt'] = $fontSize;
        }

        if (strtolower((string) $fill->getFillType()) !== 'none') {
            $fillColor = $this->argbToHex($fill->getStartColor()->getARGB());
            if ($fillColor !== null) {
                $payload['background_color'] = $fillColor;
            }
        }

        $borderPayload = [];
        foreach (['left', 'right', 'top', 'bottom'] as $side) {
            $edge = $borders->{'get' . ucfirst($side)}();
            $edgeStyle = strtolower((string) $edge->getBorderStyle());
            if ($edgeStyle === 'none' || $edgeStyle === '') {
                continue;
            }

            $borderPayload[$side] = [
                'width' => $this->mapBorderWidth($edgeStyle),
                'style' => $this->mapBorderStyle($edgeStyle),
                'color' => $this->argbToHex($edge->getColor()->getARGB()) ?? '#000000',
            ];
        }

        if (!empty($borderPayload)) {
            $payload['borders'] = $borderPayload;
        }

        $horizontal = strtolower((string) $alignment->getHorizontal());
        if ($horizontal !== '' && $horizontal !== 'general') {
            $payload['align'] = $this->mapHorizontalAlign($horizontal);
        }

        $vertical = strtolower((string) $alignment->getVertical());
        if ($vertical !== '' && $vertical !== 'bottom') {
            $payload['valign'] = $this->mapVerticalAlign($vertical);
        }

        return $payload;
    }

    private function argbToHex(?string $argb): ?string
    {
        if (!is_string($argb) || $argb === '') {
            return null;
        }

        $value = strtoupper(trim($argb));
        if (strlen($value) === 8) {
            $value = substr($value, 2);
        }

        if (!preg_match('/^[0-9A-F]{6}$/', $value)) {
            return null;
        }

        return '#' . $value;
    }

    private function mapBorderStyle(string $excelStyle): string
    {
        return match ($excelStyle) {
            'dotted' => 'dotted',
            'dashed',
            'dashdot',
            'dashdotdot',
            'mediumdashed',
            'mediumdashdot',
            'mediumdashdotdot',
            'slantdashdot' => 'dashed',
            'double' => 'double',
            default => 'solid',
        };
    }

    private function mapBorderWidth(string $excelStyle): string
    {
        return match ($excelStyle) {
            'thick', 'double' => '3px',
            'medium',
            'mediumdashed',
            'mediumdashdot',
            'mediumdashdotdot' => '2px',
            default => '1px',
        };
    }

    private function mapHorizontalAlign(string $value): string
    {
        return match ($value) {
            'center', 'centercontinuous', 'distributed', 'fill', 'justify' => 'center',
            'right' => 'right',
            default => 'left',
        };
    }

    private function mapVerticalAlign(string $value): string
    {
        return match ($value) {
            'center', 'distributed', 'justify' => 'middle',
            'top' => 'top',
            default => 'bottom',
        };
    }

    /** @return array{0: array{0:int,1:int},1: array{0:int,1:int}} */
    private function rangeBoundaries(string $range): array
    {
        $parts = explode(':', strtoupper(trim($range)));
        $start = $parts[0] ?? 'A1';
        $end = $parts[1] ?? $start;

        [$startCol, $startRow] = $this->splitCellReference($start);
        [$endCol, $endRow] = $this->splitCellReference($end);

        return [
            [$this->columnIndexFromString($startCol), $startRow],
            [$this->columnIndexFromString($endCol), $endRow],
        ];
    }

    /** @return array{0:string,1:int} */
    private function splitCellReference(string $reference): array
    {
        if (preg_match('/^([A-Z]+)(\d+)$/', $reference, $matches) !== 1) {
            return ['A', 1];
        }

        return [$matches[1], (int) $matches[2]];
    }

    private function columnIndexFromString(string $column): int
    {
        $column = strtoupper(trim($column));
        $index = 0;

        foreach (str_split($column) as $char) {
            $ord = ord($char);
            if ($ord < 65 || $ord > 90) {
                continue;
            }
            $index = ($index * 26) + ($ord - 64);
        }

        return max(1, $index);
    }

    private function stringFromColumnIndex(int $index): string
    {
        $index = max(1, $index);
        $label = '';

        while ($index > 0) {
            $index--;
            $label = chr(65 + ($index % 26)) . $label;
            $index = intdiv($index, 26);
        }

        return $label;
    }
}
