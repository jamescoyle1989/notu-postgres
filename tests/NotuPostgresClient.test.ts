import { expect, test } from 'vitest';
import { NotuPostgresClient } from '../src/NotuPostgresClient';
import { Space, Attr, Note, Tag, NotuCache } from 'notu';
import { PgConnection } from '../src/PostgresConnection';
import { NotuPostgresCacheFetcher } from '../src';
import pg from 'pg';
const { Client } = pg;


test('', async () => {
    const connectionFactory = async () => {
        const dbClient = new Client({
            user: 'james',
            password: 'password',
            host: 'localhost',
            port: 5432,
            database: 'notujames'
        });
        await dbClient.connect();
        return new PgConnection(dbClient);
    }
    
    const client = new NotuPostgresClient(
        connectionFactory,
        new NotuCache(
            new NotuPostgresCacheFetcher(connectionFactory)
        )
    );

    await client.setup();
});