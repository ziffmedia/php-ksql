<?php
use ZiffMedia\Ksql\QueryResultRow;

test('it_should_be_iterable', function() {
    $qrr = new QueryResultRow(["foo" => "bar", "bar" => "baz"]);
    expect(is_iterable($qrr))->toBeTrue();
});

test('it_should_be_countable', function() {
    $qrr = new QueryResultRow(["foo" => "bar", "bar" => "baz"]);
    expect(count($qrr))->toBe(2);
});

test('it_should_support_array_access', function() {
    $qrr = new QueryResultRow(["foo" => "bar", "bar" => "baz"]);
    expect($qrr["foo"])->toBe("bar");
});

test('it_should_support_object_access', function() {
    $qrr = new QueryResultRow(["foo" => "bar", "bar" => "baz"]);
    expect($qrr->foo)->toBe("bar");
});
