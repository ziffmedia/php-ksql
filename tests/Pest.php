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

function mockPushQueryResponse($rows)
{
    $rowVals = [];
    foreach ($rows as $row) {
        $rowVals[] = array_values($row);
    }

    $header = json_encode(
        [
            'queryId' => 'testquery123',
            'columnNames' => array_keys($rows[0]),
            'columnTypes' => deriveDataTypesFromArrayValues($rows[0]),
        ]
    );

    $body = function () use ($header, $rowVals) {
        yield $header;
        foreach ($rowVals as $rowVal) {
            yield json_encode($rowVal);
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
