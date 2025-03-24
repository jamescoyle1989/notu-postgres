import { expect, test } from 'vitest';
import { NotuPostgresClient, PostgresConnection } from '../src';


class FakePostgresConnection {
    log = [];

    run(query: string) {
        this.log.push(query);
        return this.log.length;
    }

    close(): void {
        this.log.push('CLOSED');
    }
}


test('customJob throws error if name not implemented', async () => {
    const client = new NotuPostgresClient(null, null);
    try {
        await client.customJob('abcde', null);
        throw Error('Previous line should have thrown an error');
    }
    catch (err) {
    }
});

test('customJob runs raw SQL function', async () => {
    const connection = new FakePostgresConnection();
    const client = new NotuPostgresClient(
        async () => (connection as any) as PostgresConnection,
        null
    );
    const result = await client.customJob('Raw SQL', async (cnn: PostgresConnection) => {
        await cnn.run('Get some data');
        return 'abcde';
    });

    expect(connection.log.length).toBe(2);
    expect(connection.log[0]).toBe('Get some data');
    expect(connection.log[1]).toBe('CLOSED');
    expect(result).toBe('abcde');
});

test('customJob runs raw SQL string', async () => {
    const connection = new FakePostgresConnection();
    const client = new NotuPostgresClient(
        async () => (connection as any) as PostgresConnection,
        null
    );
    const result = await client.customJob('Raw SQL', 'Get some data');

    expect(connection.log.length).toBe(2);
    expect(connection.log[0]).toBe('Get some data');
    expect(connection.log[1]).toBe('CLOSED');
    expect(result).toBe(1);
});