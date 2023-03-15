<?php

use ZiffMedia\Ksql\PushQuery;
use ZiffMedia\Ksql\ResultRow;

test('it should be iterable', function () {
    $pq = new PushQuery('test', 'SELECT * FROM foo', fn () => null);
    $r = new ResultRow($pq, ['foo' => 'string', 'bar' => 'string'], ['foo' => 'bar', 'bar' => 'baz']);
    expect(is_iterable($r))->toBeTrue();
});

test('it should be countable', function () {
    $pq = new PushQuery('test', 'SELECT * FROM foo', fn () => null);
    $r = new ResultRow($pq, ['foo' => 'string', 'bar' => 'string'], ['foo' => 'bar', 'bar' => 'baz']);
    expect(count($r))->toBe(2);
});

test('it should support array access', function () {
    $pq = new PushQuery('test', 'SELECT * FROM foo', fn () => null);
    $r = new ResultRow($pq, ['foo' => 'bar', 'bar' => 'baz']);
    expect($r['foo'])->toBe('bar')
        ->and($r['bar'])->toBe('baz');
});