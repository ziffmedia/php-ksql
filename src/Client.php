<?php

namespace ZiffMedia\Ksql;

use InvalidArgumentException;
use Symfony\Component\HttpClient\AmpHttpClient;
use Symfony\Contracts\HttpClient\HttpClientInterface;

class Client
{
    public function __construct(
        protected string $endpoint,
        protected string|null $username = null,
        protected string|null $password = null,
        protected HttpClientInterface|null $client = null
    ) {
        if (! $client) {
            $this->client = new AmpHttpClient();
        }

        $this->client = $this->client->withOptions([
            'http_version' => '2.0',
            'base_uri' => $this->endpoint,
        ]);

        if ($this->username && $this->password) {
            $authHeader = 'Basic '.base64_encode($this->username.':'.$this->password);
            $headers['Authorization'] = $authHeader;
            $this->client = $this->client->withOptions([
                'headers' => $headers,
            ]);
        }
    }

    public function query(string $query): PullQueryResult
    {
        if (stripos($query, 'emit changes') !== false) {
            throw new InvalidArgumentException('Queries sent to the query() should only be pull queries. Use stream() instead.');
        }

        if (! str_ends_with($query, ';')) {
            $query .= ';';
        }

        $response = $this->client->request('POST', '/query-stream', [
            'headers' => [
                'Accept' => 'application/json',
            ],
            'body' => json_encode([
                'sql' => $query,
            ]),
        ]);

        $rows = $response->toArray();
        $header = array_shift($rows);
        $schema = array_combine($header['columnNames'], $header['columnTypes']);
        $schema = array_map(fn ($val) => strtolower($val), $schema);

        $rows = array_map(function ($row) use ($schema) {
            return array_combine(array_keys($schema), $row);
        }, $rows);

        return new PullQueryResult(
            $query,
            $header['queryId'],
            $schema,
            $rows
        );
    }

    /**
     * @param  PushQuery|PushQuery[]  $query
     * @return void
     */
    public function stream(array|PushQuery $query): void
    {
        $queries = [];
        if (! is_array($query)) {
            $queries[$query->name] = $query;
        } else {
            foreach ($query as $q) {
                $queries[$q->name] = $q;
            }
        }

        foreach ($queries as $name => $query) {
            if (stripos($query->query, 'emit changes') === false) {
                throw new InvalidArgumentException('Queries sent to the stream() should only be push queries. Use query() instead.');
            }

            if (! str_ends_with($query->query, ';')) {
                $query->query .= ';';
            }
        }

        $responses = [];
        foreach ($queries as $query) {
            $requestBody = [
                'sql' => $query->query,
                'properties' => [
                    'auto.offset.reset' => strtolower($query->offset->name),
                ],
            ];

            $responses[] = $this->client->request('POST', '/query-stream', [
                'body' => json_encode($requestBody),
                'headers' => [
                    'Accept' => 'application/vnd.ksqlapi.delimited.v1',
                ],
                'user_data' => [
                    'query_name' => $query->name,
                ],
            ]);
        }

        $schemas = [];
        foreach ($this->client->stream($responses) as $response => $chunk) {
            $userData = $response->getInfo('user_data');
            $queryName = $userData['query_name'];
//            if ($chunk->isTimeout()) {
//                continue;
//            }
            $content = $chunk->getContent();
            if (strlen($content)) {
                $content = json_decode($content, true);
                if (is_array($content)) {
                    if (isset($content['queryId'])) {
                        $queries[$queryName]->queryId = $content['queryId'];
                        $schema = array_combine($content['columnNames'], $content['columnTypes']);
                        $schema = array_map(fn ($val) => strtolower($val), $schema);
                        $schemas[$queryName] = $schema;
                    } else {
                        $row = new PushQueryRow(
                            $queries[$queryName],
                            $schemas[$queryName],
                            array_combine(array_keys($schemas[$queryName]), $content)
                        );
                        if (is_callable($queries[$queryName]->handler)) {
                            $handler = $queries[$queryName]->handler;
                            $handler($row);
                        }
                    }
                }
            }
        }
    }
}
