<?php
use ZiffMedia\Ksql\PullQueryResult;
use ZiffMedia\Ksql\PushQueryRow;

function queryResult(): PullQueryResult
{
    $qrr = new PullQueryResult(
        "SELECT email FROM users",
        1234,
        ['email' => 'string'],
        [
            ['email' => 'admin@example.com'],
            ['email' => 'owner@example.com'],
        ]
    );
    return $qrr;
}

test('it_should_be_iterable', function() {
    expect(is_iterable(queryResult()))->toBeTrue();
});

test('it_should_be_countable', function() {
    $qrr = queryResult();
    expect(is_countable($qrr))->toBeTrue();
    expect(count($qrr))->toBe(2);
});

test('is_should_support_array_access', function() {
    expect(queryResult()[0])->toBe(['email' => 'admin@example.com']);
});