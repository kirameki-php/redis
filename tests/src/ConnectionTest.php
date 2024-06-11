<?php declare(strict_types=1);

namespace Tests\Kirameki\Redis;

use Kirameki\Collections\Vec;
use Kirameki\Redis\Config\ExtensionConfig;
use Kirameki\Redis\Exceptions\CommandException;
use Kirameki\Redis\Support\Type;
use stdClass;
use function array_keys;
use function mt_rand;
use function time;

final class ConnectionTest extends TestCase
{
    public function test_persistent_connection(): void
    {
        $conn = $this->createExtConnection('main', new ExtensionConfig('redis', persistent: true));
        $this->assertSame('hi', $conn->echo('hi'));
    }

    public function test_different_db(): void
    {
        $conn = $this->createExtConnection('main', new ExtensionConfig('redis'));
        $this->assertSame(0, $conn->clientInfo()['db']);
        $conn = $this->createExtConnection('alt', new ExtensionConfig('redis', database: 1));
        $this->assertSame(1, $conn->clientInfo()['db']);
    }

    public function test_construct__does_not_actually_connect(): void
    {
        $conn = $this->createExtConnection('main');
        $this->assertSame(false, $conn->isConnected());
    }

    public function test_connect(): void
    {
        $conn = $this->createExtConnection('main');
        $this->assertSame(false, $conn->isConnected());
        $conn->connect();
        $this->assertSame(true, $conn->isConnected());
    }

    public function test_disconnect(): void
    {
        $conn = $this->createExtConnection('main');
        $conn->connect();
        $oldId = $conn->clientInfo()['id'];
        $this->assertSame(true, $conn->disconnect());
        $this->assertSame(false, $conn->disconnect());
        $this->assertNotSame($oldId, $conn->clientInfo()['id']);
    }

    public function test_reconnect(): void
    {
        $conn = $this->createExtConnection('main');

        // reconnect from connected state
        $conn->connect();
        $id = $conn->clientInfo()['id'];
        $conn->reconnect();
        $this->assertNotSame($id, $newId = $conn->clientInfo()['id']);

        // reconnect from disconnected state
        $this->assertSame(true, $conn->disconnect());
        $conn->reconnect();
        $this->assertNotSame($newId, $conn->clientInfo()['id']);
    }

    # region CONNECTION ------------------------------------------------------------------------------------------------

    public function test_connection_clientList(): void
    {
        $conn1 = $this->createExtConnection('main');
        $conn2 = $this->createExtConnection('alt');
        $count = count($conn1->clientList());
        $this->assertCount($count + 1, $conn2->clientList());
    }

    public function test_connection_clientInfo(): void
    {
        $conn = $this->createExtConnection('main');
        $info = $conn->clientInfo();
        $this->assertIsInt($info['id']);
        $this->assertSame(0, $info['db']);
    }

    public function test_connection_clientSetname(): void
    {
        $conn = $this->createExtConnection('main');
        $name = 'test';
        $conn->clientSetname($name);
        $this->assertSame($name, $conn->clientGetname());
    }

    public function test_connection_clientGetname(): void
    {
        $conn = $this->createExtConnection('main');
        $this->assertSame(null, $conn->clientGetname());
        $name = 'test';
        $conn->clientSetname($name);
        $this->assertSame($name, $conn->clientGetname());
    }

    public function test_connection_echo(): void
    {
        $conn = $this->createExtConnection('main');
        $this->assertSame('hi', $conn->echo('hi'));
    }

    public function test_connection_ping(): void
    {
        $conn = $this->createExtConnection('main');
        $this->assertTrue($conn->ping());
    }

    # endregion CONNECTION ---------------------------------------------------------------------------------------------

    # region GENERIC ---------------------------------------------------------------------------------------------------

    public function test_generic_del(): void
    {
        $conn = $this->createExtConnection('main');
        $data = ['a' => 1, 'b' => 2];
        $keys = array_keys($data);
        $conn->mSet($data);
        $sets = $conn->mGet(...$keys);

        $this->assertSame(1, $sets['a']);
        $this->assertSame(2, $sets['b']);

        $conn->del(...$keys);

        // check removed
        $result = $conn->mGet(...$keys);
        $this->assertFalse($result['a']);
        $this->assertFalse($result['b']);
    }

    public function test_generic_exists(): void
    {
        $conn = $this->createExtConnection('main');
        $data = ['a' => 1, 'b' => 2, 'c' => false, 'd' => null];
        $keys = array_keys($data);
        $conn->mSet($data);

        // mixed result
        $result = $conn->exists(...$keys, ...['f']);
        $this->assertSame(4, $result);

        // nothing exists
        $result = $conn->exists('x', 'y', 'z');
        $this->assertSame(0, $result);

        // no arg
        $result = $conn->exists();
        $this->assertSame(0, $result);
    }

    public function test_generic_expireTime(): void
    {
        $conn = $this->createExtConnection('main');
        $conn->set('a', 1);
        $this->assertNull($conn->expireTime('a'));
        $conn->expire('a', 5);
        $this->assertGreaterThan(time(), $conn->expireTime('a'));
        $this->assertFalse($conn->expireTime('b'));
    }

    public function test_generic_pExpireTime(): void
    {
        $conn = $this->createExtConnection('main');
        $conn->set('a', 1);
        $this->assertNull($conn->pExpireTime('a'));
        $conn->pExpire('a', 5);
        $this->assertGreaterThan(time() * 1000, $conn->pExpireTime('a'));
        $this->assertFalse($conn->pExpireTime('b'));
    }

    public function test_generic_persist(): void
    {
        $conn = $this->createExtConnection('main');
        $conn->set('a', 1, ['ex' => 5]);
        $conn->set('b', 1);
        $this->assertTrue($conn->persist('a'));
        $this->assertFalse($conn->persist('b'));
        $this->assertFalse($conn->persist('c'));
    }

    public function test_generic_expire(): void
    {
        $conn = $this->createExtConnection('main');
        $conn->set('a', 1);
        $conn->expire('a', 5);
        $this->assertSame(5, $conn->ttl('a'), 'expire with seconds');

        $conn->set('b', 1);
        $conn->expire('b', 2, 'nx');
        $this->assertSame(2, $conn->ttl('b'), 'nx on no ttl yet');
        $conn->expire('b', 5, 'nx');
        $this->assertSame(2, $conn->ttl('b'), 'nx on existing ttl');

        $conn->set('c', 1);
        $conn->expire('c', 10, 'xx');
        $this->assertSame(null, $conn->ttl('c'), 'xx on no ttl');
        $conn->set('d', 1);
        $conn->expire('d', 10);
        $this->assertSame(10, $conn->ttl('d'), 'xx on existing ttl');

        $conn->set('e', 1);
        $conn->expire('e', 10, 'gt');
        $this->assertNull($conn->ttl('e'), 'gt on no ttl');
        $conn->expire('e', 5);
        $conn->expire('e', 10, 'gt');
        $this->assertSame(10, $conn->ttl('e'), 'gt on existing ttl');
        $conn->expire('e', 5, 'gt');
        $this->assertSame(10, $conn->ttl('e'), 'gt on existing ttl with smaller expire time');

        $conn->set('f', 1);
        $conn->expire('f', 20, 'lt');
        $this->assertSame(20, $conn->ttl('f'), 'lt on no ttl');
        $conn->expire('f', 15);
        $conn->expire('f', 10, 'lt');
        $this->assertSame(10, $conn->ttl('f'), 'lt on existing ttl');
        $conn->expire('f', 15, 'lt');
        $this->assertSame(10, $conn->ttl('f'), 'lt on existing ttl with bigger expire time');
    }

    public function test_generic_pexpire(): void
    {
        $conn = $this->createExtConnection('main');
        $conn->set('a', 1);
        $conn->pExpire('a', 5);
        $this->assertLessThan(5, $conn->ttl('a'), 'expire with seconds');

        $conn->set('b', 1);
        $conn->pExpire('b', 2, 'nx');
        $this->assertLessThan(2, $conn->ttl('b'), 'nx on no ttl yet');
        $conn->pExpire('b', 5, 'nx');
        $this->assertLessThan(2, $conn->ttl('b'), 'nx on existing ttl');

        $conn->set('c', 1);
        $conn->pExpire('c', 10, 'xx');
        $this->assertSame(null, $conn->ttl('c'), 'xx on no ttl');
        $conn->set('d', 1);
        $conn->pExpire('d', 10);
        $this->assertLessThan(10, $conn->ttl('d'), 'xx on existing ttl');

        $conn->set('e', 1);
        $conn->pExpire('e', 10, 'gt');
        $this->assertNull($conn->ttl('e'), 'gt on no ttl');
        $conn->pExpire('e', 5);
        $conn->pExpire('e', 10, 'gt');
        $this->assertLessThan(10, $conn->ttl('e'), 'gt on existing ttl');
        $conn->pExpire('e', 5, 'gt');
        $this->assertLessThan(10, $conn->ttl('e'), 'gt on existing ttl with smaller expire time');

        $conn->set('f', 1);
        $conn->pExpire('f', 20, 'lt');
        $this->assertLessThan(20, $conn->ttl('f'), 'lt on no ttl');
        $conn->pExpire('f', 15);
        $conn->pExpire('f', 10, 'lt');
        $this->assertLessThan(10, $conn->ttl('f'), 'lt on existing ttl');
        $conn->pExpire('f', 15, 'lt');
        $this->assertLessThan(10, $conn->ttl('f'), 'lt on existing ttl with bigger expire time');
    }

    public function test_generic_randomKey(): void
    {
        $conn = $this->createExtConnection('main');
        $this->assertSame(null, $conn->randomKey());
        $conn->set('test', 1);
        $this->assertSame('test', $conn->randomKey());
    }

    public function test_generic_rename(): void
    {
        $conn = $this->createExtConnection('main');
        $conn->set('test', 1);
        $this->assertTrue($conn->rename('test', 'renamed'));
    }

    public function test_generic_rename_key_not_exists(): void
    {
        $this->expectException(CommandException::class);
        $this->expectExceptionMessage('ERR no such key');
        $conn = $this->createExtConnection('main');
        $this->assertFalse($conn->rename('miss', 'renamed'));
    }

    public function test_generic_renameNx(): void
    {
        $conn = $this->createExtConnection('main');
        $conn->set('test', 1);
        $conn->set('abc', 2);
        $this->assertTrue($conn->renameNx('test', 'renamed'));
        $this->assertFalse($conn->renameNx('renamed', 'abc'));
        $this->assertSame(1, $conn->get('renamed'));
        $this->assertSame(2, $conn->get('abc'));
    }

    public function test_generic_renameNx_key_not_exists(): void
    {
        $this->expectException(CommandException::class);
        $this->expectExceptionMessage('ERR no such key');
        $conn = $this->createExtConnection('main');
        $this->assertFalse($conn->renameNx('miss', 'renamed'));
    }

    public function test_generic_scan(): void
    {
        $conn = $this->createExtConnection('main');
        $data = ['a1' => 1, 'a2' => 2, '_a3' => false, 'a4' => null];
        $conn->mSet($data);

        // full scan
        $this->assertSame(['_a3', 'a1', 'a2', 'a4'], $conn->scan()->sortAsc()->toArray());

        // scan with count
        $this->assertSame(['_a3', 'a1', 'a2', 'a4'], $conn->scan(count: 1)->sortAsc()->toArray());

        // filtered with wild card
        $this->assertSame(['a1', 'a2', 'a4'], $conn->scan('a*')->sortAsc()->toArray());

        $connAlt = $this->createExtConnection('alt', new ExtensionConfig('redis', prefix: 'alt:'));

        // filtered with prefix
        $connAlt->mSet(['a5' => 5]);
        $this->assertSame(['a5'], $connAlt->scan('a*')->toArray());
        $this->assertSame(['a5'], $connAlt->scan()->toArray());

        // filtered with prefix and return prefixed
        $this->assertSame(['alt:a5'], $connAlt->scan('a*', prefixed: true)->toArray());
    }

    public function test_generic_ttl(): void
    {
        $conn = $this->createExtConnection('main');
        $conn->set('a', 1, ['ex' => 5]);
        $conn->set('b', 1);
        $this->assertSame(5, $conn->ttl('a'));
        $this->assertNull($conn->ttl('b'));
        $this->assertFalse($conn->ttl('c'));
    }

    public function test_generic_pTtl(): void
    {
        $conn = $this->createExtConnection('main');
        $conn->set('a', 1, ['px' => 500]);
        $conn->set('b', 1);
        $this->assertGreaterThan(1, $conn->pTtl('a'));
        $this->assertNull($conn->pTtl('b'));
        $this->assertFalse($conn->pTtl('c'));
    }

    public function test_generic_type(): void
    {
        $conn = $this->createExtConnection('main');
        $this->assertSame(Type::None, $conn->type('none'));
        $conn->set('string', '');
        $this->assertSame(Type::String, $conn->type('string'));
        $conn->lPush('list', 1);
        $this->assertSame(Type::List, $conn->type('list'));
        // TODO add test for non handled types
    }

    public function test_unlink(): void
    {
        $conn = $this->createExtConnection('main');
        $data = ['a' => 1, 'b' => 2, 'c' => 3];
        $conn->mSet($data);
        $this->assertSame(2, $conn->unlink('a', 'b', 'x', 'y', 'z'));
        $this->assertSame(0, $conn->unlink('a', 'b'));
        $this->assertSame(1, $conn->unlink('c', 'd'));
    }

    # endregion GENERIC ---------------------------------------------------------------------------------------------------

    # region SERVER ----------------------------------------------------------------------------------------------------

    public function test_server_dbSize(): void
    {
        $conn = $this->createExtConnection('main');
        $this->assertSame(0, $conn->dbSize());
        $conn->mSet(['a' => 1, 'b' => 2]);
        $this->assertSame(2, $conn->dbSize());
    }

    # endregion SERVER -------------------------------------------------------------------------------------------------

    # region STRING ----------------------------------------------------------------------------------------------------

    public function test_string_decr(): void
    {
        $conn = $this->createExtConnection('main');
        $this->assertSame(-1, $conn->decr('d'));
        $this->assertSame(-3, $conn->decr('d', 2));
        $this->assertSame(-1, $conn->decr('d', -2));
    }

    public function test_string_decrByFloat(): void
    {
        $conn = $this->createExtConnection('main');
        $this->assertSame(-1.0, $conn->decrByFloat('d', 1));
        $this->assertSame(-3.2, $conn->decrByFloat('d', 2.2));
        $this->assertSame(-1.0, $conn->decrByFloat('d', -2.2));
    }

    public function test_string_get(): void
    {
        $conn = $this->createExtConnection('main');
        $conn->mSet(['d' => 'abc', 'e' => null]);
        $this->assertSame('abc', $conn->get('d'));
        $this->assertSame(null, $conn->get('e'));
        $this->assertSame(false, $conn->get('f'));
    }

    public function test_string_incr(): void
    {
        $conn = $this->createExtConnection('main');
        $this->assertSame(1, $conn->incr('d'));
        $this->assertSame(3, $conn->incr('d', 2));
        $this->assertSame(1, $conn->incr('d', -2));
    }

    public function test_string_incrByFloat(): void
    {
        $conn = $this->createExtConnection('main');
        $this->assertSame(1.0, $conn->incrByFloat('d', 1));
        $this->assertSame(3.2, $conn->incrByFloat('d', 2.2));
        $this->assertSame(1.0, $conn->incrByFloat('d', -2.2));
    }

    public function test_string_mGet(): void
    {
        $conn = $this->createExtConnection('main');
        $pairs = ['a' => true, 'b' => mt_rand(), 'c' => 0.01, 'd' => 'abc', 'e' => null];
        $conn->mSet($pairs);
        $this->assertSame($pairs, $conn->mGet(...array_keys($pairs)));
    }

    public function test_string_mGet_with_array(): void
    {
        $conn = $this->createExtConnection('main');
        $pairs = ['arr' => ['a' => 1, 'b' => 2]];
        $conn->mSet($pairs);
        $this->assertSame($pairs, $conn->mGet('arr'));
    }

    public function test_string_mGet_with_object(): void
    {
        $conn = $this->createExtConnection('main');
        $object = new stdClass();
        $object->a = 1;
        $pairs = ['o' => $object];
        $conn->mSet($pairs);
        $this->assertSame($object->a, $conn->mGet('o')['o']->a);
        $this->assertSame([], $conn->mGet());
    }

    public function test_string_mSet(): void
    {
        $conn = $this->createExtConnection('main');
        $pairs = ['a1' => mt_rand(), 'a2' => mt_rand()];
        $conn->mSet($pairs);
        $this->assertSame($pairs, $conn->mGet('a1', 'a2'));
    }

    public function test_string_mSet_with_array(): void
    {
        $conn = $this->createExtConnection('main');
        $pairs = ['arr' => ['a' => 1, 'b' => 2]];
        $conn->mSet($pairs);
        $this->assertSame($pairs, $conn->mGet('arr'));
    }

    public function test_string_mSet_with_object(): void
    {
        $conn = $this->createExtConnection('main');
        $object = new stdClass();
        $object->a = 1;
        $pairs = ['o' => $object];
        $conn->mSet($pairs);
        $this->assertSame($object->a, $conn->mGet('o')['o']->a);
    }

    public function test_string_mSet_without_args(): void
    {
        $conn = $this->createExtConnection('main');
        $conn->mSet([]);
        $this->assertSame([], $conn->scan('*')->all());
    }

    # endregion STRING -------------------------------------------------------------------------------------------------

    # region LIST ------------------------------------------------------------------------------------------------------

    public function test_list_blPop(): void
    {
        $conn = $this->createExtConnection('main');
        $this->assertSame(2, $conn->lPush('l', 'abc', 1));
        $this->assertSame(['l' => 1], $conn->blPop(['l'], 100));
        $this->assertSame(['l' => 'abc'], $conn->blPop(['l'], 100));
        $this->assertSame(null, $conn->blPop(['l'], 0.01));
    }

    public function test_list_blPop__iterable_type(): void
    {
        $conn = $this->createExtConnection('main');
        $this->assertSame(1, $conn->lPush('l', 1));
        $this->assertSame(['l' => 1], $conn->blPop(new Vec(['l']), 100));
    }

    public function test_list_blPop_key_not_a_list(): void
    {
        $this->expectException(CommandException::class);
        $this->expectExceptionMessage('WRONGTYPE Operation against a key holding the wrong kind of value');
        $conn = $this->createExtConnection('main');
        $conn->set('l', 1);
        $conn->blPop(['l'], 0.01);
    }

    public function test_list_lIndex(): void
    {
        $conn = $this->createExtConnection('main');
        $this->assertSame(2, $conn->lPush('l', 'abc', 1));
        $this->assertSame('abc', $conn->lIndex('l', 1));
        $this->assertSame('abc', $conn->lIndex('l', -1));
        $this->assertFalse($conn->lIndex('l', 2)); // no index found
        $this->assertFalse($conn->lIndex('m', -1)); // no key found
    }

    public function test_list_lIndex_key_not_a_list(): void
    {
        $this->expectException(CommandException::class);
        $this->expectExceptionMessage('WRONGTYPE Operation against a key holding the wrong kind of value');
        $conn = $this->createExtConnection('main');
        $conn->set('l', 1);
        $conn->lIndex('l', 2);
    }

    public function test_list_lPush(): void
    {
        $conn = $this->createExtConnection('main');
        $this->assertSame(2, $conn->lPush('l', 'abc', 1));
        $this->assertSame(3, $conn->lPush('l', 2));
        $this->assertSame('abc', $conn->lIndex('l', 2));
        $this->assertSame(1, $conn->lIndex('l', 1));
        $this->assertSame(2, $conn->lIndex('l', 0));
    }

    public function test_list_lPush_key_not_a_list(): void
    {
        $this->expectException(CommandException::class);
        $this->expectExceptionMessage('WRONGTYPE Operation against a key holding the wrong kind of value');
        $conn = $this->createExtConnection('main');
        $conn->set('l', 1);
        $conn->lPush('l', 2);
    }

    public function test_list_rPush(): void
    {
        $conn = $this->createExtConnection('main');
        $this->assertSame(2, $conn->rPush('l', 'abc', 1));
        $this->assertSame(3, $conn->rPush('l', 2));
        $this->assertSame('abc', $conn->lIndex('l', 0));
        $this->assertSame(1, $conn->lIndex('l', 1));
        $this->assertSame(2, $conn->lIndex('l', 2));
    }

    public function test_list_rPush_key_not_a_list(): void
    {
        $this->expectException(CommandException::class);
        $this->expectExceptionMessage('WRONGTYPE Operation against a key holding the wrong kind of value');
        $conn = $this->createExtConnection('main');
        $conn->set('l', 1);
        $conn->rPush('l', 2);
    }

    # endregion LIST ---------------------------------------------------------------------------------------------------
}
