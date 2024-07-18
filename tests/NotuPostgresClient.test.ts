import { expect, test } from 'vitest';
import { MockConnection, newAttr, newNote, newSpace, testCacheFetcher } from './TestHelpers';
import { NotuPostgresClient } from '../src';
import { NotuCache } from 'notu';



test('Saves date attr properly', async () => {
    const dateAttr = newAttr('Date', 123).clean();
    const space = newSpace('Space', 1).clean();
    const note = newNote('Test', 234).in(space).clean();
    note.addAttr(dateAttr, new Date(2024, 6, 18));
    const connection = new MockConnection();
    connection.nextRunOutput = [];

    const client = new NotuPostgresClient(
        () => Promise.resolve(connection as any),
        new NotuCache(testCacheFetcher() as any)
    );
    await client.saveNotes([note]);

    expect(connection.history.length).toBe(1);
    expect(connection.history[0].command).toBe(`INSERT INTO NoteAttr (noteId, attrId, value, tagId) VALUES ($1, $2, $3, $4)`);
    expect(connection.history[0].args[0]).toBe(234);
    expect(connection.history[0].args[1]).toBe(123);
    expect(connection.history[0].args[2]).toBe('Thu Jul 18 2024 00:00:00 GMT-0400 (Eastern Daylight Time)');
    expect(connection.history[0].args[3]).toBeNull();
});