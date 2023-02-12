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
     * @return PullQueryResult[]|null
     * @throws \Symfony\Contracts\HttpClient\Exception\TransportExceptionInterface
     */
    public function query(string $query): PullQueryResult
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
                'sql' => $query
            ])
        ]);

        $rows = $response->toArray();
        $header = array_shift($rows);
        $schema = array_combine($header['columnNames'], $header['columnTypes']);
        $schema = array_map(fn($val) => strtolower($val), $schema);

        $rows = array_map(function($row) use ($schema) {
            return array_combine(array_keys($schema), $row);
        }, $rows);

        $result = new PullQueryResult(
            $query,
            $header["queryId"],
            $schema,
            $rows
        );
        return $result;
    }

    /**
     * @param string|string[] $query
     * @param callable|callable[] $handler
     * @param Offset $offset
     * @return void
     */
    public function stream(string|array $query, callable|array $handler, Offset $offset = Offset::Earliest)
    {
        if(!is_array($query)) {
            $query = ['query' => $query];
        }

        foreach ($query as &$q) {
            if (stripos($q, 'emit changes') == false) {
                throw new \InvalidArgumentException("Queries sent to the stream() should only be push queries. Use query() instead.");
            }
        }



        $responses = [];
        foreach ($query as $name => $sql) {
            $requestBody = [
                'sql' => $sql,
                'streamsProperties' => [
                    'streams.auto.offset.reset' => $offset->value
                ]
            ];

            $responses[] = $this->client->request('POST', '/query-streams', [
                'body' => json_encode($requestBody),
                'headers' => [
                    'Accept' => 'application/vnd.ksqlapi.delimited.v1'
                ],
                'user_data' => [
                    'query_name' => $name,
                ]
            ]);
        }

        $headers = [];
        foreach ($this->client->stream($responses) as $response => $chunk) {
            $userData = $response->getInfo('user_data');
            $queryName = $userData["query_name"];
            $content = $chunk->getContent();
            if (strlen($content)) {
                $content = json_decode($content, true);
                if (is_array($content)) {
                    if (isset($content['queryId'])) {
                        $headers[$queryName]["queryId"] = $content["queryId"];
                        $schema = array_combine($content['columnNames'], $content['columnTypes']);
                        $schema = array_map(fn($val) => strtolower($val), $schema);
                        $headers[$queryName]["schema"] = $schema;
                    } else {
                        $row = new PushQueryRow(
                            $queryName,
                            $query[$queryName],
                            $headers[$queryName]["queryId"],
                            $headers[$queryName]["schema"],
                            array_combine(array_keys($headers[$queryName]["schema"]), $content)
                        );
                        if (is_array($handler)) {
                            $handler[$queryName]($row);
                        } else {
                            $handler($row);
                        }
                    }
                }
            }
        }
    }
}