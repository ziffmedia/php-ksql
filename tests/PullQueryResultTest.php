<?php

use ZiffMedia\Ksql\PullQueryResult;

function queryResult(): PullQueryResult
{
    $qrr = new PullQueryResult(
        'SELECT email FROM users',
        1234,
        ['email' => 'string'],
        [
            ['email' => 'admin@example.com'],
            ['email' => 'owner@example.com'],
        ]
    );

    return $qrr;
}

test('it_should_force_lower_case_keys', function () {
    $qrr = new PullQueryResult(
        'SELECT email FROM users',
        1234,
        ['FOO' => 'string'],
        [
            ['FOO' => 'admin@example.com'],
            ['FOO' => 'owner@example.com'],
        ]
    );
    expect(array_keys($qrr->schema))->toBe(['foo']);
    foreach ($qrr as $row) {
        expect(array_keys($row))->toBe(['foo']);
    }
});

test('it_should_be_iterable', function () {
    expect(is_iterable(queryResult()))->toBeTrue();
});

test('it_should_be_countable', function () {
    $qrr = queryResult();
    expect(is_countable($qrr))->toBeTrue();
    expect(count($qrr))->toBe(2);
});

test('is_should_support_array_access', function () {
    expect(queryResult()[0])->toBe(['email' => 'admin@example.com']);
});
