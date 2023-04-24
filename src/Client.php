<?php

namespace ZiffMedia\Ksql;

use InvalidArgumentException;
use Symfony\Component\HttpClient\Exception\TransportException;
use Symfony\Component\HttpClient\HttpClient;
use Symfony\Contracts\HttpClient\HttpClientInterface;

class Client
{
    protected bool $retryOnNetworkErrors = true;

    public function __construct(
        protected string $endpoint,
        protected string|null $username = null,
        protected string|null $password = null,
        protected HttpClientInterface|null $client = null
    ) {
        if (! $client) {
            $this->client = HttpClient::create();
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

    public function retryOnNetworkErrors(bool $retry = true): void
    {
        $this->retryOnNetworkErrors = $retry;
    }

    /**
     * @return array|ResultRow[]
     */
    public function query(string|PullQuery $query): array
    {
        if (is_string($query)) {
            $query = new PullQuery($query);
        }

        if (stripos($query->query, 'emit changes') !== false) {
            throw new InvalidArgumentException('Queries sent to the query() should only be pull queries. Use stream() instead.');
        }

        if (! str_ends_with($query->query, ';')) {
            $query->query .= ';';
        }

        $response = $this->client->request('POST', '/query-stream', [
            'headers' => [
                'Accept' => 'application/json',
            ],
            'body' => json_encode([
                'sql' => $query->query,
            ]),
        ]);

        $rows = $response->toArray();
        $header = array_shift($rows);
        $query->queryId = $header['queryId'];

        $schema = array_combine($header['columnNames'], $header['columnTypes']);
        $query->schema = $schema;

        $rows = array_map(function ($row) use ($schema, $query) {
            return new ResultRow($query, array_combine(array_keys($schema), $row));
        }, $rows);

        return $rows;
    }

    /**
     * @param  PushQuery|PushQuery[]  $query
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



        do {
            $hasThrown = false;
            $pendingResponses = [];
            foreach ($queries as $query) {
                $requestBody = [
                    'sql' => $query->query,
                    'properties' => [
                        'auto.offset.reset' => strtolower($query->offset->name),
                    ],
                ];

                $pendingResponses[] = $this->client->request('POST', '/query-stream', [
                    'body' => json_encode($requestBody),
                    'headers' => [
                        'Accept' => 'application/vnd.ksqlapi.delimited.v1',
                    ],
                    'user_data' => [
                        'query_name' => $query->name,
                    ],
                ]);
            }

            $responseStream = $this->client->stream($pendingResponses);

            while ($responseStream->valid()) {
                $chunk = $responseStream->current();
                $response = $responseStream->key();
                $userData = $response->getInfo('user_data');
                $queryName = $userData['query_name'];
                $query = $queries[$queryName];

                try {
                    if ($chunk->isTimeout()) {
                        $responseStream = $this->client->stream($pendingResponses);

                        continue;
                    }

                    $content = $chunk->getContent();
                    if (strlen($content)) {
                        $content = json_decode($content, true);
                        if (is_array($content)) {
                            if (isset($content['queryId'])) {
                                $query->queryId = $content['queryId'];
                                $schema = array_combine($content['columnNames'], $content['columnTypes']);
                                $query->schema = $schema;
                            } else {
                                $row = new ResultRow(
                                    $query,
                                    array_combine(array_keys($query->schema), $content)
                                );
                                if (is_callable($query->handler)) {
                                    ($query->handler)($row);
                                }
                            }
                        }
                    }
                    $responseStream->next();
                } catch (TransportException $e) {
                    if (!$this->retryOnNetworkErrors) {
                        throw $e;
                    } else {
                        $hasThrown = true;
                        break;
                    }
                }
            }
        } while ($this->retryOnNetworkErrors && $hasThrown);
    }
}
