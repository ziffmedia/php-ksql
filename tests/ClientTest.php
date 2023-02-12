<?php
use ZiffMedia\Ksql\Client;
use ZiffMedia\Ksql\PullQueryResult;
use ZiffMedia\Ksql\PushQueryRow;
use Symfony\Component\HttpClient\MockHttpClient;
use Symfony\Component\HttpClient\Response\MockResponse;

test('it_creates_auth_headers', function() {
    $r = mockPullQueryResponse([["foo" => "bar"]]);
    $m = new MockHttpClient([$r]);
    $c = new Client("http://localhost", "user", "pass", $m);
    try {
        $c->query("SELECT * FROM test");
    } catch (\Exception $e) {
        // don't care if the client actually handles this request properly, only care if the request is right
    }
    $expectedHeader = "Authorization: Basic " . base64_encode("user:pass");
    expect($r->getRequestOptions()['headers'])->toContain($expectedHeader);
});

test('it_does_not_create_auth_headers', function() {
    $r = mockPullQueryResponse([["foo" => "bar"]]);
    $m = new MockHttpClient([$r]);
    $c = new Client(endpoint: "http://localhost", client: $m);
    try {
        $c->query("SELECT * FROM test");
    } catch (\Exception $e) {
        // don't care if the client actually handles this request properly, only care if the request is right
    }
    $expectedHeader = "Authorization: Basic " . base64_encode("user:pass");
    expect($r->getRequestOptions()['headers'])->not()->toContain($expectedHeader);
});

test('it_runs_simple_pull_queries', function() {
    $r = mockPullQueryResponse([["foo" => "bar"], ["foo" => "baz"]]);
    $m = new MockHttpClient([$r]);
    $c = new Client(endpoint: "http://localhost", client: $m);
    $result = $c->query("SELECT * FROM test;");
    expect(count($result))->toBe(2);
    expect($result)->toBeInstanceOf(PullQueryResult::class);
    expect($result[0])->toBeArray();
    expect($result[0]['foo'])->toBe('bar');
    expect($result[1]['foo'])->toBe('baz');
    expect($result->schema['foo'])->toBeString();
    expect($result->queryId)->toBe('testquery123');
});

test('it_sends_proper_pull_query_content_type_header', function() {
    $r = mockPullQueryResponse([["foo" => "bar"]]);
    $m = new MockHttpClient([$r]);
    $c = new Client("http://localhost", "user", "pass", $m);
    try {
        $c->query("SELECT * FROM test");
    } catch (\Exception $e) {
        // don't care if the client actually handles this request properly, only care if the request is right
    }
    $expectedHeader = "Accept: application/json";
    expect($r->getRequestOptions()['headers'])->toContain($expectedHeader);
});

test('it_throws_when_using_push_queries_on_query_method', function() {
    $r = mockPullQueryResponse([["foo" => "bar"], ["foo" => "baz"]]);
    $m = new MockHttpClient([$r]);
    $c = new Client(endpoint: "http://localhost", client: $m);
   expect(fn() => $c->query("SELECT * FROM foo EMIT CHANGES"))->toThrow(InvalidArgumentException::class);
});

test('it_throws_when_using_pull_queries_on_stream_method', function() {
    $r = mockPullQueryResponse([["foo" => "bar"], ["foo" => "baz"]]);
    $m = new MockHttpClient([$r]);
    $c = new Client(endpoint: "http://localhost", client: $m);
    expect(fn() => $c->stream("SELECT * FROM foo", fn() => null))->toThrow(InvalidArgumentException::class);
});

test('it_properly_delimits_pull_queries', function() {
    $r = mockPullQueryResponse([["foo" => "bar"], ["foo" => "baz"]]);
    $m = new MockHttpClient([$r]);
    $c = new Client(endpoint: "http://localhost", client: $m);
    $result = $c->query("SELECT * FROM foo");
    expect($result->query)->toBe("SELECT * FROM foo;");
});

test('it_sends_proper_push_query_content_type_header', function() {
    $r = mockPushQueryResponse([["foo" => "bar"]]);
    $m = new MockHttpClient([$r]);
    $c = new Client("http://localhost", "user", "pass", $m);
    try {
        $c->stream("SELECT * FROM test EMIT CHANGES", fn() => null);
    } catch (\Exception $e) {
        // don't care if the client actually handles this request properly, only care if the request is right
    }
    $expectedHeader = "Accept: application/vnd.ksqlapi.delimited.v1";
    expect($r->getRequestOptions()['headers'])->toContain($expectedHeader);
});

test("it_runs_simple_push_queries", function() {
    $data = [["foo" => "bar"], ["foo" => "baz"]];
    $r = mockPushQueryResponse($data);
    $m = new MockHttpClient([$r]);
    $c = new Client(endpoint: "http://localhost", client: $m);
    $handler = function($row) use (&$data) {
        $expected = current($data);
        expect($row["foo"])->toBe($expected["foo"]);
        next($data);
    };
    $c->stream("SELECT * FROM test EMIT CHANGES", $handler);
});
