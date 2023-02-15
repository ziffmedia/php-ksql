<?php

use ZiffMedia\Ksql\PushQuery;
use ZiffMedia\Ksql\PushQueryRow;

test('it_should_be_iterable', function () {
    $pq = new PushQuery('test', 'SELECT * FROM foo', fn () => null);
    $r = new PushQueryRow($pq, ['foo' => 'string', 'bar' => 'string'], ['foo' => 'bar', 'bar' => 'baz']);
    expect(is_iterable($r))->toBeTrue();
});

test('it_should_be_countable', function () {
    $pq = new PushQuery('test', 'SELECT * FROM foo', fn () => null);
    $r = new PushQueryRow($pq, ['foo' => 'string', 'bar' => 'string'], ['foo' => 'bar', 'bar' => 'baz']);
    expect(count($r))->toBe(2);
});

test('it_should_support_array_access', function () {
    $pq = new PushQuery('test', 'SELECT * FROM foo', fn () => null);
    $r = new PushQueryRow($pq, ['foo' => 'string', 'bar' => 'string'], ['foo' => 'bar', 'bar' => 'baz']);
    expect($r['foo'])->toBe('bar');
    expect($r['bar'])->toBe('baz');
});

test('it_should_force_lower_case_keys', function () {
    $pq = new PushQuery('test', 'SELECT * FROM foo', fn () => null);
    $qrr = new PushQueryRow(
        $pq,
        ['FOO' => 'string'],
        ['FOO' => 'admin@example.com']
    );
    expect(array_keys($qrr->schema))
        ->toBe(['foo'])
        ->and(array_keys($qrr->data))
        ->toBe(['foo']);
});
