<?php

namespace ZiffMedia\Ksql;

use Error;
use Exception;
use InvalidArgumentException;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Symfony\Component\HttpClient\Exception\TransportException;
use Symfony\Component\HttpClient\HttpClient;
use Symfony\Contracts\HttpClient\HttpClientInterface;
use ZiffMedia\Ksql\Parser\ApplicationJsonParser;
use ZiffMedia\Ksql\Parser\DelimittedParser;
use ZiffMedia\Ksql\Parser\ParserInterface;
use ZiffMedia\Ksql\Parser\V1JsonParser;

class Client
{
    protected bool $retryOnNetworkErrors = true;

    protected ContentType $acceptContenType = ContentType::V1_DELIMITTED;

    public function __construct(
        protected string $endpoint,
        protected ?string $username = null,
        protected ?string $password = null,
        protected ?HttpClientInterface $client = null,
        protected ?LoggerInterface $logger = null
    ) {
        if (! $client) {
            $this->client = HttpClient::create();
        }

        if (! $this->logger) {
            $this->logger = new NullLogger();
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

    public function setAcceptContentType(ContentType $contentType)
    {
        $this->acceptContenType = $contentType;
    }

    public function setLogger(LoggerInterface $logger)
    {
        $this->logger = $logger;
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

        $this->logger->info('KSQL QUERY: Preparing to execute query '.$query->query);
        $response = $this->client->request('POST', '/query-stream', [
            'headers' => [
                'Accept' => ContentType::APPLICATION_JSON->value,
            ],
            'body' => json_encode([
                'sql' => $query->query,
            ]),
        ]);

        $rows = $response->toArray();
        $this->logger->debug('KSQL QUERY: received '.count($rows).' query rows');
        $header = array_shift($rows);
        $query->queryId = $header['queryId'];
        $this->logger->debug('KSQL QUERY: discovered query id '.$query->queryId);

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
        $this->logger->debug('KSQL STREAM: using content type '.$this->acceptContenType->value);
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
                $this->logger->info('KSQL STREAM: Preparing to stream '.$query->query.' (offset: '.$query->offset->name.') as named query '.$query->name);
                $requestBody = [
                    'sql' => $query->query,
                    'properties' => [
                        'auto.offset.reset' => strtolower($query->offset->name),
                    ],
                ];

                $pendingResponses[] = $this->client->request('POST', '/query-stream', [
                    'body' => json_encode($requestBody),
                    'headers' => [
                        'Accept' => $this->acceptContenType->value,
                    ],
                    'user_data' => [
                        'query_name' => $query->name,
                    ],
                ]);
            }

            $responseStream = $this->client->stream($pendingResponses);
            $this->logger->debug('KSQL STREAM: executed '.count($pendingResponses).' stream queries');

            while ($responseStream->valid()) {
                $chunk = $responseStream->current();
                $response = $responseStream->key();
                $userData = $response->getInfo('user_data');
                $queryName = $userData['query_name'];
                $query = $queries[$queryName];
                $this->logger->debug('KSQL STREAM: received stream chunk for '.$queryName);

                try {
                    if ($chunk->isTimeout()) {
                        $responseStream = $this->client->stream($pendingResponses);
                        $this->logger->debug('KSQL STREAM: received network timeout chunk for '.$queryName.', restreaming all queries');

                        continue;
                    }

                    $content = $chunk->getContent();
                    if (strlen($content)) {
                        $content = $this->parseContent($content);
                        if (is_array($content)) {
                            $this->logger->debug('KSQL STREAM: succesfully parsed chunk for '.$queryName);
                            if (isset($content['queryId'])) {
                                $query->queryId = $content['queryId'];
                                $schema = array_combine($content['columnNames'], $content['columnTypes']);
                                $query->schema = $schema;
                                $this->logger->debug('KSQL STREAM: chunk for '.$queryName.' is a header, discovered query id: '.$query->queryId);
                            } else {
                                if (isset($content['tombstone'])) {
                                    $this->logger->debug('KSQL STREAM: received tombstone for '.$queryName);
                                    $row = new TombstoneRow(
                                        $query,
                                        array_combine(array_keys($query->schema), $content['tombstone'])
                                    );
                                    $keySearch = array_filter($content['tombstone']);
                                    $key = empty($keySearch) ? null : $keySearch[0];
                                    $row->key = $key;
                                } else {
                                    $this->logger->debug('KSQL STREAM: received record for '.$queryName);
                                    $row = new ResultRow(
                                        $query,
                                        array_combine(array_keys($query->schema), $content)
                                    );
                                }

                                if (is_callable($query->handler)) {
                                    ($query->handler)($row);
                                }
                            }
                        }
                    } else {
                        $this->logger->debug('KSQL STREAM: chunk for '.$queryName.' had no content');
                    }
                    $responseStream->next();
                } catch (TransportException $e) {
                    $this->logger->warning('KSQL STREAM: transport exception - '.$e->getMessage());
                    if (! $this->retryOnNetworkErrors) {
                        throw $e;
                    } else {
                        $this->logger->info('KSQL STREAM: re-starting streams due to transport exception');
                        $hasThrown = true;
                        break;
                    }
                }
            }
        } while ($this->retryOnNetworkErrors && $hasThrown);
    }

    private function parseContent(string $content)
    {
        /** @var ParserInterface $parser */
        $parser = match ($this->acceptContenType) {
            ContentType::APPLICATION_JSON => ApplicationJsonParser::class,
            ContentType::V1_DELIMITTED => DelimittedParser::class,
            ContentType::V1_JSON => V1JsonParser::class
        };

        try {
            $parsed = $parser::parse($content);
        } catch (Exception|Error $e) {
            $this->logger->warning('KSQL STREAM: failed to parse content: '.$content);
            $parsed = null;
        }

        return $parsed;
    }
}
