<?php
use ZiffMedia\Ksql\PushQueryRow;

test('it_should_be_iterable', function() {
    $r = new PushQueryRow("SELECT * FROM foo", "test123", ["foo" => "string", "bar" => "string"], ["foo" => "bar", "bar" => "baz"]);
    expect(is_iterable($r))->toBeTrue();
});

test('it_should_be_countable', function() {
    $r = new PushQueryRow("SELECT * FROM foo", "test123", ["foo" => "string", "bar" => "string"], ["foo" => "bar", "bar" => "baz"]);
    expect(count($r))->toBe(2);
});

test('it_should_support_array_access', function() {
    $r = new PushQueryRow("SELECT * FROM foo", "test123", ["foo" => "string", "bar" => "string"], ["foo" => "bar", "bar" => "baz"]);
    expect($r["foo"])->toBe("bar");
    expect($r["bar"])->toBe("baz");
});
