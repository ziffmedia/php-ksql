<?php

use ZiffMedia\Ksql\Parser\V1JsonParser;

test('it should parse header lines correctly', function () {
    $line = '[{"header":{"queryId":"test123","schema":"`COL1` INTEGER, `KEY` STRING"}},';
    expect(V1JsonParser::parse($line))->toBeArray();
});

test('it should parse row lines correctly', function () {
    $line = '{"row":{"columns":["foo","bar", "baz"]}},';
    expect(V1JsonParser::parse($line))->toBeArray();
});

test('it should normalize schema string to key values', function () {
    $line = '[{"header":{"queryId":"test123","schema":"`COL1` INTEGER, `KEY` STRING"}},';
    expect(V1JsonParser::parse($line))->toBe([
        'queryId' => 'test123',
        'columnNames' => ['COL1', 'KEY'],
        'columnTypes' => ['INTEGER', 'STRING'],
    ]);
});

test('it should normalize rows to plain arrays', function () {
    $line = '{"row":{"columns":["foo","bar", "baz"]}},';
    expect(V1JsonParser::parse($line))->toBe(['foo', 'bar', 'baz']);
});

test('it should handle tombstone rows', function () {
    $line = '{"row":{"columns":["bar", null, null], "tombstone": true}},';
    expect(V1JsonParser::parse($line))->toBe(['tombstone' => ['bar', null, null]]);
});
