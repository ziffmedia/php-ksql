<?php

namespace ZiffMedia\Ksql\Parser;

class V1JsonParser implements ParserInterface
{
    public static function parse(string $data): array
    {
        $data = trim($data);
        if (str_starts_with($data, '[{')) {
            $data = substr($data, 1, -1);
            $data = json_decode($data, true);

            $cols = explode(',', $data['header']['schema']);
            $columnNames = [];
            $columnTypes = [];
            foreach ($cols as $col) {
                $parts = explode('`', $col);
                $columnNames[] = $parts[1];
                $columnTypes[] = trim($parts[2]);
            }
            $transformed = [
                'queryId' => $data['header']['queryId'],
                'columnNames' => $columnNames,
                'columnTypes' => $columnTypes,
            ];

            return $transformed;
        } else {
            $data = substr($data, 0, -1);
            $row = json_decode($data, true);
            if (isset($row['row']['tombstone'])) {
                return ['tombstone' => $row['row']['columns']];
            }

            return $row['row']['columns'];
        }
    }
}
