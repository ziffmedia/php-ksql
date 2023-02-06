<?php
namespace ZiffMedia\Ksql;

use Symfony\Component\HttpClient\Exception\ServerException;
use Symfony\Component\HttpClient\HttpClient;
use Symfony\Contracts\HttpClient\HttpClientInterface;

class Client
{
    public function __construct(
        protected string $endpoint,
        protected string|null $username = null,
        protected string|null $password = null,
        protected HttpClientInterface|null $client = null
    ) {
        if (!$client) {
            $this->client = HttpClient::create();
        }

        $this->client = $this->client->withOptions([
            'http_version' => '2.0',
            'base_uri' =>  $this->endpoint,
        ]);

        if ($this->username && $this->password) {
            $authHeader = "Basic " . base64_encode($this->username . ":" . $this->password);
            $headers['Authorization'] = $authHeader;
            $this->client = $this->client->withOptions([
                'headers' => $headers
            ]);
        }
    }

    public function setHttpClient(HttpClientInterface $client): void
    {
        $this->client = $client;
    }


    /**
     * @param string $query
     * @param callable|null $handler
     * @return QueryResult[]|null
     * @throws \Symfony\Contracts\HttpClient\Exception\TransportExceptionInterface
     */
    public function query(string $query): QueryResult
    {
        if (stripos($query, 'emit changes') !== false) {
            throw new \InvalidArgumentException("Queries sent to the query() should only be pull queries. Use stream() instead.");
        }

        if (!str_ends_with($query,';')) {
            $query .= ';';
        }

        $response = $this->client->request('POST', "/query-streams", [
            'headers' => [
                'Accept' => 'application/json'
            ],
            'body' => json_encode([
                'ksql' => $query
            ])
        ]);

        $rows = $response->toArray();
        array_pop($rows);
        $header = array_shift($rows)["header"];
        $schema = $header["schema"];
        $columns = [];
        foreach (explode(',', $schema) as $col) {
            $split = explode('`', $col);
            $columns[strtolower($split[1])] = trim(strtolower($split[2]));
        }

        $qrData = [];
        foreach ($rows as $row) {
            $qrr = new QueryResultRow(array_combine(array_keys($columns), $row["row"]["columns"]));
            $qrData[] = $qrr;
        }
        $result = new QueryResult(
            $query,
            $header["queryId"],
            $columns,
            $qrData
        );
        return $result;
    }

    public function stream(string $query, callable $handler, Offset $offset = Offset::Earliest, $maxRecords = false)
    {
        $recordCounter = 0;

        $headers = [
            'Accept' => 'application/vnd.ksqlapi.delimited.v1'
        ];
        $fullRequestUri = $this->endpoint . '/query-stream';

        $responses = [];
        foreach ($this->streamingQueries as $name => $config) {
            $query = $config["query"];
            $eventClass = $config["event_class"] ?? KsqlStreamChanged::class;
            $requestBody = [
                'sql' => $query,
                'streamsProperties' => [
                    'streams.auto.offset.reset' => $offset->value
                ]
            ];

            $responses[] = $client->request('POST', $fullRequestUri, [
                'body' => json_encode($requestBody),
                'user_data' => [
                    'query_name' => $name,
                    'event_class' => $eventClass
                ]
            ]);
        }

        try {
            foreach ($client->stream($responses) as $response => $chunk) {
                dump($client);
                $userData = $response->getInfo('user_data');
                if (!$chunk->isFirst() ||  $chunk->isLast()) {
                    dump($chunk->getContent());
                }
            }
        } catch (ServerException $e) {
            // @todo do some laravel shit here
            throw $e;
        }



    }
}