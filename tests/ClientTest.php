<?php
use ZiffMedia\Ksql\Client;
use ZiffMedia\Ksql\QueryResult;
use ZiffMedia\Ksql\QueryResultRow;
use Symfony\Component\HttpClient\MockHttpClient;
use Symfony\Component\HttpClient\Response\MockResponse;

test('it_creates_auth_headers', function() {
    $r = new MockResponse('...', ['http_code' => 200]);
    $m = new MockHttpClient([$r]);
    $c = new Client("http://localhost", "user", "pass", $m);
    $c->query("SELECT * FROM test");
    dd($r->getRequestOptions()['headers']);
});


test('it_can_issue_simple_queries_on_streams', function() {
    $client = new Client("http://localhost:8088");
    $result = $client->query("SELECT * FROM users1;");
    expect(count($result))->toBe(500);
    expect($result)->toBeInstanceOf(QueryResult::class);
    expect($result[0])->toBeInstanceOf(QueryResultRow::class);
})->skip();

test('it_throws_when_using_push_queries_on_query_method', function() {
   expect((new Client('http://localhost:8888'))->query("SELECT * FROM foo EMIT CHANGES"))->toThrow(InvalidArgumentException::class);
})->skip();

test('it_properly_delimits_queries', function() {
    $client = new Client("http://localhost:8088");
    $result = $client->query("SELECT * FROM users1 LIMIT 1");
    expect($result)->toBeInstanceOf(QueryResult::class);
})->skip();

test('it_properly_populates_query_result_objects', function() {
    $query = "SELECT * FROM users1 LIMIT 1;";
    $client = new Client("http://localhost:8088");
    $result = $client->query($query);
    expect($result->schema)->toBe(['userid' => 'string', 'gender' => 'string']);
    expect($result->query)->toBe($query);
    expect($result[0])->toBeInstanceOf(QueryResultRow::class);
})->skip();

test('it_call_row_handler_on_query_method', function() {
    $query = "SELECT * FROM users1 LIMIT 5;";
    $client = new Client("http://localhost:8088");
    $counter = 0;
    $client->query($query, function(QueryResultRow $qr) use (&$counter) {
        $counter++;
    });
    expect($counter)->toBe(5);
})->skip();
