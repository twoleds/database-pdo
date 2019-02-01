<?php

namespace Twoleds\Database\PDO;

use Twoleds\Database\DatabaseException;
use Twoleds\Database\DatabaseInterface;

/**
 * Class PDODatabase implements simple interface for accessing databases via PDO extension.
 *
 * @package Twoleds\Database\PDO
 */
class PDODatabase implements DatabaseInterface
{
    /** @var \PDO|callable */
    private $connection;

    /**
     * PDODatabase constructor.
     *
     * @param string $dsn
     * @param string|null $user
     * @param string|null $password
     * @param array $attributes
     */
    public function __construct($dsn, $user = null, $password = null, $attributes = [])
    {
        $this->connection = function () use ($dsn, $user, $password, $attributes) {
            $attributes[\PDO::ATTR_ERRMODE] = \PDO::ERRMODE_EXCEPTION;
            return new \PDO($dsn, $user, $password, $attributes);
        };
    }

    /**
     * Returns connection to the database server. Only first call initializes
     * a new connection to the database server.
     *
     * @return \PDO
     *
     * @throws DatabaseException
     */
    private function connect(): \PDO
    {
        if (is_callable($this->connection)) {
            try {
                $this->connection = ($this->connection)();
            } catch (\PDOException $exception) {
                throw new PDOException($exception->getMessage(), $exception->getCode(), $exception);
            }
        }
        return $this->connection;
    }

    /**
     * Prepares & executes the SQL and call handler for processing result.
     *
     * @param callable $handler
     * @param string $sql
     * @param mixed ...$params
     *
     * @return mixed
     *
     * @throws DatabaseException
     */
    private function execute($handler, $sql, ...$params)
    {
        $connection = $this->connect();

        try {
            $statement = $connection->prepare($sql);
        } catch (\PDOException $exception) {
            throw new PDOException($exception->getMessage(), $exception->getCode(), $exception);
        }

        try {
            $statement->execute($params);
            return $handler($statement, $connection);
        } catch (\PDOException $exception) {
            throw new PDOException($exception->getMessage(), $exception->getCode(), $exception);
        } finally {
            $statement->closeCursor();
        }
    }

    /**
     * @inheritdoc
     */
    public function insert($sql, ...$params)
    {
        return $this->execute(function (\PDOStatement $statement, \PDO $connection) {
            return $connection->lastInsertId();
        }, $sql, ...$params);
    }

    /**
     * @inheritdoc
     */
    public function select($sql, ...$params)
    {
        return $this->execute(function (\PDOStatement $statement, \PDO $connection) {
            return $statement->fetchAll(\PDO::FETCH_ASSOC);
        }, $sql, ...$params);
    }

    /**
     * @inheritdoc
     */
    public function selectField($sql, ...$params)
    {
        return $this->execute(function (\PDOStatement $statement, \PDO $connection) {
            return $statement->fetchColumn();
        }, $sql, ...$params);
    }

    /**
     * @inheritdoc
     */
    public function selectRow($sql, ...$params)
    {
        return $this->execute(function (\PDOStatement $statement, \PDO $connection) {
            return $statement->fetch(\PDO::FETCH_ASSOC) ?: null;
        }, $sql, ...$params);
    }

    /**
     * @inheritdoc
     */
    public function transactional($callback)
    {
        $result = null;
        $connection = $this->connect();
        try {
            $connection->beginTransaction();
            $result = $callback($this);
            $connection->commit();
        } catch (\Throwable $exception) {
            $connection->rollBack();
            throw new PDOException('An exception thrown during transaction.', $exception->getCode(), $exception);
        }
        return $result;
    }

    /**
     * @inheritdoc
     */
    public function update($sql, ...$params)
    {
        return $this->execute(function (\PDOStatement $statement, \PDO $connection) {
            return $statement->rowCount();
        }, $sql, ...$params);
    }
}
