<?php

use Symfony\Component\HttpClient\Exception\TransportException;
use Symfony\Component\HttpClient\MockHttpClient;
use ZiffMedia\Ksql\Client;
use ZiffMedia\Ksql\Offset;
use ZiffMedia\Ksql\PushQuery;
use ZiffMedia\Ksql\ResultRow;
use ZiffMedia\Ksql\ContentType;
use ZiffMedia\Ksql\TombstoneRow;

test('it uses the streaming api', function () {
    $r = mockPullQueryResponse([['foo' => 'bar']]);
    $m = new MockHttpClient([$r]);
    $c = new Client('http://localhost', 'user', 'pass', $m);
    try {
        $c->query('SELECT * FROM test');
    } catch (Exception $e) {
        // don't care if the client actually handles this request properly, only care if the request is right
    }
    expect($r->getRequestUrl())->toBe('http://localhost/query-stream');

    $r = mockDelimittedPushQueryResponse([['foo' => 'bar']]);
    $m = new MockHttpClient([$r]);
    $c = new Client('http://localhost', 'user', 'pass', $m);
    $pq = new PushQuery('test', 'SELECT * FROM foo EMIT CHANGES', fn () => null);
    try {
        $c->stream($pq);
    } catch (Exception $e) {
        // don't care if the client actually handles this request properly, only care if the request is right
    }
    expect($r->getRequestUrl())->toBe('http://localhost/query-stream');
});

test('it creates auth headers', function () {
    $r = mockPullQueryResponse([['foo' => 'bar']]);
    $m = new MockHttpClient([$r]);
    $c = new Client('http://localhost', 'user', 'pass', $m);
    try {
        $c->query('SELECT * FROM test');
    } catch (Exception $e) {
        // don't care if the client actually handles this request properly, only care if the request is right
    }
    $expectedHeader = 'Authorization: Basic '.base64_encode('user:pass');
    expect($r->getRequestOptions()['headers'])->toContain($expectedHeader);
});

test('it does not create auth headers', function () {
    $r = mockPullQueryResponse([['foo' => 'bar']]);
    $m = new MockHttpClient([$r]);
    $c = new Client(endpoint: 'http://localhost', client: $m);
    try {
        $c->query('SELECT * FROM test');
    } catch (Exception $e) {
        // don't care if the client actually handles this request properly, only care if the request is right
    }
    $expectedHeader = 'Authorization: Basic '.base64_encode('user:pass');
    expect($r->getRequestOptions()['headers'])->not()->toContain($expectedHeader);
});

test('it runs simple pull queries', function () {
    $r = mockPullQueryResponse([['foo' => 'bar'], ['foo' => 'baz']]);
    $m = new MockHttpClient([$r]);
    $c = new Client(endpoint: 'http://localhost', client: $m);
    $result = $c->query('SELECT * FROM test;');
    expect(count($result))->toBe(2)
        ->and($result)->toBeArray()
        ->and($result[0])->toBeInstanceOf(ResultRow::class)
        ->and($result[0]['foo'])->toBe('bar')
        ->and($result[1]['foo'])->toBe('baz')
        ->and($result[0]->query->schema['foo'])->toBeString()
        ->and($result[0]->query->queryId)->toBe('testquery123');
});

test('it sends proper pull query content type header', function () {
    $r = mockPullQueryResponse([['foo' => 'bar']]);
    $m = new MockHttpClient([$r]);
    $c = new Client('http://localhost', 'user', 'pass', $m);
    try {
        $c->query('SELECT * FROM test');
    } catch (Exception $e) {
        // don't care if the client actually handles this request properly, only care if the request is right
    }
    $expectedHeader = 'Accept: application/json';
    expect($r->getRequestOptions()['headers'])->toContain($expectedHeader);
});

test('it throws when using push queries on query method', function () {
    $r = mockPullQueryResponse([['foo' => 'bar'], ['foo' => 'baz']]);
    $m = new MockHttpClient([$r]);
    $c = new Client(endpoint: 'http://localhost', client: $m);
    expect(fn () => $c->query('SELECT * FROM foo EMIT CHANGES'))->toThrow(InvalidArgumentException::class);
});

test('it throws when using pull queries on stream method', function () {
    $r = mockPullQueryResponse([['foo' => 'bar'], ['foo' => 'baz']]);
    $m = new MockHttpClient([$r]);
    $c = new Client(endpoint: 'http://localhost', client: $m);
    $pq = new PushQuery('test', 'SELECT * FROM foo', fn () => null);
    expect(fn () => $c->stream($pq))->toThrow(InvalidArgumentException::class);
});

test('it properly delimits pull queries', function () {
    $r = mockPullQueryResponse([['foo' => 'bar'], ['foo' => 'baz']]);
    $m = new MockHttpClient([$r]);
    $c = new Client(endpoint: 'http://localhost', client: $m);
    $result = $c->query('SELECT * FROM foo');
    expect($result[0]->query->query)->toBe('SELECT * FROM foo;');
});

test('it sends proper push query content type header', function () {
    $r = mockDelimittedPushQueryResponse([['foo' => 'bar']]);
    $m = new MockHttpClient([$r]);
    $c = new Client('http://localhost', 'user', 'pass', $m);
    $pq = new PushQuery('test', 'SELECT * FROM foo EMIT CHANGES', fn () => null);
    try {
        $c->stream($pq);
    } catch (Exception $e) {
        // don't care if the client actually handles this request properly, only care if the request is right
    }
    $expectedHeader = 'Accept: application/vnd.ksqlapi.delimited.v1';
    expect($r->getRequestOptions()['headers'])->toContain($expectedHeader);
});

test('it properly delimits push queries', function () {
    $r = mockDelimittedPushQueryResponse([['foo' => 'bar'], ['foo' => 'baz']]);
    $m = new MockHttpClient([$r]);
    $c = new Client(endpoint: 'http://localhost', client: $m);
    $pq = new PushQuery('test', 'SELECT * FROM foo EMIT CHANGES', function (ResultRow $r) {
        expect($r->query->query)->toBe('SELECT * FROM foo EMIT CHANGES;');
    });
    $c->stream($pq);
});

test('it runs simple push queries', function () {
    $data = [['foo' => 'bar'], ['foo' => 'baz']];
    $r = mockDelimittedPushQueryResponse($data);
    $m = new MockHttpClient([$r]);
    $c = new Client(endpoint: 'http://localhost', client: $m);
    $pq = new PushQuery('test', 'SELECT * FROM foo EMIT CHANGES', function ($row) use (&$data) {
        $expected = current($data);
        expect($row['foo'])->toBe($expected['foo']);
        next($data);
    });
    $c->stream($pq);
});

test('it runs multiplexed stream queries with matched handlers', function () {
    $data1 = [['foo' => 'bar'], ['foo' => 'baz']];
    $data2 = [['bar' => 'baz'], ['bar' => 'foo']];

    $r1 = mockDelimittedPushQueryResponse($data1);
    $r2 = mockDelimittedPushQueryResponse($data2);

    $m = new MockHttpClient([$r1, $r2]);
    $c = new Client(endpoint: 'http://localhost', client: $m);

    $handler1 = function ($row) use (&$data1) {
        $expected = current($data1);
        expect($row['foo'])->toBe($expected['foo']);
        next($data1);
    };

    $handler2 = function ($row) use (&$data2) {
        $expected = current($data2);
        expect($row['bar'])->toBe($expected['bar']);
        next($data2);
    };

    $pq1 = new PushQuery('data1', 'SELECT * FROM test EMIT CHANGES', $handler1);
    $pq2 = new PushQuery('data2', 'SELECT * FROM bar EMIT CHANGES', $handler2);

    $c->stream(
        [
            $pq1,
            $pq2,
        ]
    );
});

test('it runs multiplexed stream queries with a single handler', function () {
    $data1 = [['foo' => 'bar'], ['foo' => 'baz']];
    $data2 = [['bar' => 'baz'], ['bar' => 'foo']];

    $r1 = mockDelimittedPushQueryResponse($data1);
    $r2 = mockDelimittedPushQueryResponse($data2);

    $m = new MockHttpClient([$r1, $r2]);
    $c = new Client(endpoint: 'http://localhost', client: $m);

    $handler = function (ResultRow $row) use (&$data1, &$data2) {
        $expected = current(${$row->query->name});
        expect($row[key($expected)])->toBe($expected[key($expected)]);
        next(${$row->query->name});
    };

    $pq1 = new PushQuery('data1', 'SELECT * FROM test EMIT CHANGES', $handler);
    $pq2 = new PushQuery('data2', 'SELECT * FROM bar EMIT CHANGES', $handler);

    $c->stream([$pq1, $pq2]);
});

test('it obeys offsets on push queries', function () {
    $r = mockDelimittedPushQueryResponse([['foo' => 'bar']]);
    $m = new MockHttpClient([$r]);
    $c = new Client('http://localhost', 'user', 'pass', $m);
    $pq = new PushQuery('test', 'SELECT * FROM foo EMIT CHANGES', fn () => null, Offset::LATEST);
    try {
        $c->stream($pq);
    } catch (Exception $e) {
        // don't care if the client actually handles this request properly, only care if the request is right
    }
    expect($r->getRequestOptions()['body'])->toContain('auto.offset');
    expect($r->getRequestOptions()['body'])->toContain('latest');
});

test('it should handle idle timeouts correctly', function () {
    $data = [['foo' => 'bar'], '', ['foo' => 'baz']];
    $r = mockDelimittedPushQueryResponse($data);
    $m = new MockHttpClient([$r]);
    $c = new Client(endpoint: 'http://localhost', client: $m);
    $responseCount = 0;
    $pq = new PushQuery('test', 'SELECT * FROM foo EMIT CHANGES', function ($row) use (&$data, &$responseCount) {
        $responseCount++;
        $expected = current($data);
        if ($expected === '') {
            next($data);
            $expected = current($data); // skip testing the timeout, which we will never receive
        }
        expect($row['foo'])->toBe($expected['foo']);
        next($data);
    });
    $c->stream($pq);
    expect($responseCount)->toBe(2);
});

test('it should handle multiplexed idle timeouts correctly', function () {
    $data1 = [['foo' => 'bar'], '', ['foo' => 'baz']];
    $data2 = [['foo' => 'bar'], ['foo' => 'baz'], ''];
    $data3 = [['foo' => 'bar'], ['foo' => 'baz']];

    $r1 = mockDelimittedPushQueryResponse($data1);
    $r2 = mockDelimittedPushQueryResponse($data2);
    $r3 = mockDelimittedPushQueryResponse($data3);

    $m = new MockHttpClient([$r1, $r2, $r3]);
    $c = new Client(endpoint: 'http://localhost', client: $m);

    $responseCount = 0;

    $handler = function (ResultRow $row) use (&$data1, &$data2, &$data3, &$responseCount) {
        $varName = $row->query->name;
        $responseCount++;
        $expected = current($$varName);
        if ($expected === '') {
            next($$varName);
            $expected = current($$varName); // skip testing the timeout, which we will never receive
        }
        expect($row['foo'])->toBe($expected['foo']);
        next($$varName);
    };

    $pq1 = new PushQuery('data1', 'SELECT * FROM foo EMIT CHANGES', $handler);
    $pq2 = new PushQuery('data2', 'SELECT * FROM bar EMIT CHANGES', $handler);
    $pq3 = new PushQuery('data3', 'SELECT * FROM baz EMIT CHANGES', $handler);

    $c->stream([$pq1, $pq2, $pq3]);
    expect($responseCount)->toBe(6);
});

test('it should handle transport exceptions correctly', function () {
    $data1 = [['foo' => 'bar'], new RuntimeException('connection closed')];
    $data2 = [['foo' => 'baz']];
    $expectedData = array_merge($data1, $data2);
    $r1 = mockDelimittedPushQueryResponse($data1);
    $r2 = mockDelimittedPushQueryResponse($data2);
    $m = new MockHttpClient([$r1, $r2]);
    $c = new Client(endpoint: 'http://localhost', client: $m);
    $responseCount = 0;
    $pq = new PushQuery('test', 'SELECT * FROM foo EMIT CHANGES', function ($row) use (&$expectedData, &$responseCount) {
        $responseCount++;
        $expected = current($expectedData);
        if ($expected instanceof Exception) {
            next($expectedData);
            $expected = current($expectedData); // skip testing the timeout, which we will never receive
        }
        expect($row['foo'])->toBe($expected['foo']);
        next($expectedData);
    });
    $c->stream($pq);
    expect($responseCount)->toBe(2);
});

test('it should fail on unhandled transport exceptions when retry is false', function () {
    $data = [['foo' => 'bar'], new RuntimeException('connection closed')];
    $r = mockDelimittedPushQueryResponse($data);
    $m = new MockHttpClient([$r]);
    $c = new Client(endpoint: 'http://localhost', client: $m);
    $pq = new PushQuery('test', 'SELECT * FROM foo EMIT CHANGES', fn () => null);
    expect(fn () => $c->stream($pq))->toThrow(TransportException::class);
});

test('it should emit tombstone objects for tombstone rows', function() {
    $data = [['tombstone' => ['key' => '123', 'foo' => null]]];
    $r = mockV1JsonPushQueryResponse($data);
    $m = new MockHttpClient([$r]);
    $c = new Client(endpoint: 'http://localhost', client: $m);
    $c->setAcceptContentType(ContentType::V1_JSON);
    $pq = new PushQuery('test', 'SELECT * FROM foo EMIT CHANGES', function ($row) use (&$data) {
        expect($row)->toBeInstanceOf(TombstoneRow::class);
        expect($row->key)->toBe('123');
    });
    $c->stream($pq);
});

test('it should handle all content type options', function() {
    $data = [['foo' => 'bar'], ['foo' => 'baz']];

    $r = mockDelimittedPushQueryResponse($data);
    $m = new MockHttpClient([$r]);
    $c = new Client(endpoint: 'http://localhost', client: $m);
    $c->setAcceptContentType(ContentType::V1_DELIMITTED);
    $pq = new PushQuery('test', 'SELECT * FROM foo EMIT CHANGES', function ($row) use (&$data) {
        $expected = current($data);
        expect($row['foo'])->toBe($expected['foo']);
        next($data);
    });
    $c->stream($pq);
    reset($data);

    $r = mockV1JsonPushQueryResponse($data);
    $m = new MockHttpClient([$r]);
    $c = new Client(endpoint: 'http://localhost', client: $m);
    $c->setAcceptContentType(ContentType::V1_JSON);
    $pq = new PushQuery('test', 'SELECT * FROM foo EMIT CHANGES', function ($row) use (&$data) {
        $expected = current($data);
        expect($row['foo'])->toBe($expected['foo']);
        next($data);
    });
    $c->stream($pq);
    reset($data);

    $r = mockApplicationJsonPushQueryResponse($data);
    $m = new MockHttpClient([$r]);
    $c = new Client(endpoint: 'http://localhost', client: $m);
    $c->setAcceptContentType(ContentType::APPLICATION_JSON);
    $pq = new PushQuery('test', 'SELECT * FROM foo EMIT CHANGES', function ($row) use (&$data) {
        $expected = current($data);
        expect($row['foo'])->toBe($expected['foo']);
        next($data);
    });
    $c->stream($pq);
    reset($data);
});