<?php

use Symfony\Component\HttpClient\Response\MockResponse;

function mockPullQueryResponse($rows)
{
    $rowVals = [];
    foreach ($rows as $row) {
        $rowVals[] = array_values($row);
    }
    array_unshift($rowVals,
        [
            'queryId' => 'testquery123',
            'columnNames' => array_keys($rows[0]),
            'columnTypes' => deriveDataTypesFromArrayValues($rows[0]),
        ]
    );

    $body = json_encode($rowVals);

    return new MockResponse($body, ['http_code' => 200]);
}

function mockDelimittedPushQueryResponse($rows)
{
    $rowVals = [];
    foreach ($rows as $row) {
        if (is_array($row)) {
            $rowVals[] = array_values($row);
        } else {
            $rowVals[] = $row;
        }
    }

    $header = json_encode(
        [
            'queryId' => 'testquery'.rand(1, 10),
            'columnNames' => array_keys($rows[0]),
            'columnTypes' => deriveDataTypesFromArrayValues($rows[0]),
        ]
    );

    $body = function () use ($header, $rowVals) {
        yield $header;
        foreach ($rowVals as $rowVal) {
            if (is_array($rowVal)) {
                yield json_encode($rowVal);
            } else {
                yield $rowVal; // allow for timeout testing
            }
        }
    };

    return new MockResponse($body(), ['http_code' => 200]);
}

function mockApplicationJsonPushQueryResponse($rows)
{
    $rowVals = [];
    foreach ($rows as $row) {
        if (is_array($row)) {
            $rowVals[] = array_values($row);
        } else {
            $rowVals[] = $row;
        }
    }

    $header = json_encode(
        [
            'queryId' => 'testquery'.rand(1, 10),
            'columnNames' => array_keys($rows[0]),
            'columnTypes' => deriveDataTypesFromArrayValues($rows[0]),
        ]
    );
    $header = '['.$header.',';

    $body = function () use ($header, $rowVals) {
        yield $header;
        foreach ($rowVals as $rowVal) {
            if (is_array($rowVal)) {
                yield json_encode($rowVal).',';
            } else {
                yield $rowVal; // allow for timeout testing
            }
        }
    };

    return new MockResponse($body(), ['http_code' => 200]);
}

function mockV1JsonPushQueryResponse($rows)
{
    $testRow = $rows[0]['tombstone'] ?? $rows[0];
    $schemaCols = array_keys($testRow);
    $schemaTypes = deriveDataTypesFromArrayValues($testRow);
    $schemaParts = [];
    for ($i = 0; $i < count($schemaCols); $i++) {
        $schemaParts[] = '`'.$schemaCols[$i].'` '.$schemaTypes[$i];
    }
    $schema = implode(', ', $schemaParts);
    $header = json_encode(
        ['header' => [
            'queryId' => 'testquery'.rand(1, 10),
            'schema' => $schema,
        ]]
    );
    $header = '['.$header.','.PHP_EOL;

    $body = function () use ($header, $rows) {
        yield $header;
        foreach ($rows as $row) {
            if (is_array($row)) {
                if (isset($row['tombstone'])) {
                    $rowData['row']['tombstone'] = true;
                    $rowData['row']['columns'] = array_values($row['tombstone']);
                } else {
                    $rowData['row']['columns'] = array_values($row);
                }
                yield json_encode($rowData).',';
            } else {
                yield $row; // allow for timeout testing
            }
        }
    };

    return new MockResponse($body(), ['http_code' => 200]);
}

function deriveDataTypesFromArrayValues($row)
{
    $types = [];
    foreach (array_values($row) as $val) {
        $types[] = strtoupper(get_debug_type($val));
    }

    return $types;
}
