<?php
use ZiffMedia\Ksql\Parser\DelimittedParser;

test("it should parse header lines correctly", function() {
    $line = '{"queryId": "123", "columnNames": ["test1"], "columnTypes": ["STRING"]}' . PHP_EOL;
    expect(DelimittedParser::parse($line))->toBe([
        "queryId" => "123",
        "columnNames" => ["test1"],
        "columnTypes" => ["STRING"]
    ]);
});

test("it should parse row lines correctly", function() {
    $line = '["foo", "bar", "baz"]' . PHP_EOL;
    expect(DelimittedParser::parse($line))->toBe(['foo', 'bar', 'baz']);
});