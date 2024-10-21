import { expect, test } from 'vitest';
import { NotuCache, NewParsedQuery, NewParsedTag, ParsedTag, NewParsedTagFilter } from 'notu';
import { buildNewNotesQuery } from '../src/NewPostgresQueryBuilder';
import { testCacheFetcher } from './TestHelpers';


async function newNotuCache(): Promise<NotuCache> {
    const cache = new NotuCache(testCacheFetcher());
    await cache.populate();
    return cache;
}


test('buildNotesQuery correctly processes empty query', async () => {
    const query = new NewParsedQuery();

    expect(buildNewNotesQuery(query, 1, await newNotuCache()))
        .toBe('SELECT n.id, n.spaceId, n.text, n.date FROM Note n LEFT JOIN Tag t ON n.id = t.id WHERE n.spaceId = 1;');
});

test('buildNotesQuery correctly processes query with order clause', async () => {
    const query = new NewParsedQuery();
    query.order = 'date';

    expect(buildNewNotesQuery(query, 1, await newNotuCache()))
        .toBe('SELECT n.id, n.spaceId, n.text, n.date FROM Note n LEFT JOIN Tag t ON n.id = t.id WHERE n.spaceId = 1 ORDER BY date;');
});

test('buildNotesQuery correctly processes query with self tag filter', async () => {
    const query = new NewParsedQuery();
    query.where = '{tag0}';
    query.tags.push((() => {
        const tag = new NewParsedTag();
        tag.name = 'Tag 1';
        tag.space = null;
        tag.searchDepths = [0];
        return tag;
    })());

    expect(buildNewNotesQuery(query, 1, await newNotuCache()))
        .toBe(
            'SELECT n.id, n.spaceId, n.text, n.date ' +
            'FROM Note n LEFT JOIN Tag t ON n.id = t.id ' +
            'WHERE n.spaceId = 1 AND (n.id = 1);'
        );
});

test('buildNotesQuery correctly processes query with child tag filter', async () => {
    const query = new NewParsedQuery();
    query.where = '{tag0}';
    query.tags.push((() => {
        const tag = new NewParsedTag();
        tag.name = 'Tag 1';
        tag.space = null;
        tag.searchDepths = [1];
        return tag;
    })());

    expect(buildNewNotesQuery(query, 1, await newNotuCache()))
        .toBe(
            'SELECT n.id, n.spaceId, n.text, n.date ' +
            'FROM Note n LEFT JOIN Tag t ON n.id = t.id ' +
            'WHERE n.spaceId = 1 AND (EXISTS(SELECT 1 FROM NoteTag nt WHERE nt.noteId = n.id AND nt.tagId = 1));'
        );
});

test('buildNotesQuery correctly processes query with child tag filter', async () => {
    const query = new NewParsedQuery();
    query.where = '{tag0}';
    query.tags.push((() => {
        const tag = new NewParsedTag();
        tag.name = 'Tag 1';
        tag.space = null;
        tag.searchDepths = [0,1];
        return tag;
    })());

    expect(buildNewNotesQuery(query, 1, await newNotuCache()))
        .toBe(
            'SELECT n.id, n.spaceId, n.text, n.date ' +
            'FROM Note n LEFT JOIN Tag t ON n.id = t.id ' +
            'WHERE n.spaceId = 1 AND ' +
            '((n.id = 1 OR EXISTS(SELECT 1 FROM NoteTag nt WHERE nt.noteId = n.id AND nt.tagId = 1)));'
        );
});

test('buildNotesQuery can search for matches 2 relations deep', async () => {
    const query = new NewParsedQuery();
    query.where = '{tag0}';
    query.tags.push((() => {
        const tag = new NewParsedTag();
        tag.name = 'Tag 3';
        tag.space = null;
        tag.searchDepths = [2]
        return tag;
    })());

    expect(buildNewNotesQuery(query, 1, await newNotuCache()))
        .toBe(
            'SELECT n.id, n.spaceId, n.text, n.date ' +
            'FROM Note n LEFT JOIN Tag t ON n.id = t.id ' +
            'WHERE n.spaceId = 1 AND ' +
            '(EXISTS(SELECT 1 FROM NoteTag nt1 INNER JOIN NoteTag nt2 ON nt2.noteId = nt1.tagId WHERE nt1.noteId = n.id AND nt2.tagId = 3));'
        );
});

test('buildNotesQuery correctly processes query with tag filter', async () => {
    const query = new NewParsedQuery();
    query.where = '{tag0}';
    query.tags.push((() => {
        const tag = new NewParsedTag();
        tag.name = 'Tag 3';
        tag.space = null;
        tag.searchDepths = [1];
        tag.filter = new NewParsedTagFilter();
        tag.filter.pattern = '{exp0} < 5';
        tag.filter.exps = ['beans.count'];
        return tag;
    })());

    expect(buildNewNotesQuery(query, 1, await newNotuCache()))
        .toBe(
            'SELECT n.id, n.spaceId, n.text, n.date ' +
            'FROM Note n LEFT JOIN Tag t ON n.id = t.id ' +
            'WHERE n.spaceId = 1 AND ' +
            `(EXISTS(SELECT 1 FROM NoteTag nt WHERE nt.noteId = n.id AND nt.tagId = 3 AND (nt.data->'beans'->>'count' < 5)));`
        );
});

test('buildNotesQuery can handle {Now}', async () => {
    const query = new NewParsedQuery();
    query.where = 'date > {Now}';

    const result = buildNewNotesQuery(query, 1, await newNotuCache());

    expect(result).toContain('date > NOW()');
});

test('buildNotesQuery can handle {Today}', async () => {
    const query = new NewParsedQuery();
    query.where = 'date > {Today}';

    const result = buildNewNotesQuery(query, 1, await newNotuCache());

    expect(result).toContain(`date > CURRENT_DATE`);
});

test('buildNotesQuery can handle {Yesterday}', async () => {
    const query = new NewParsedQuery();
    query.where = 'date > {Yesterday}';

    const result = buildNewNotesQuery(query, 1, await newNotuCache());

    expect(result).toContain(`date > (CURRENT_DATE - INTERVAL'1d')`);
});

test('buildNotesQuery can handle {Tomorrow}', async () => {
    const query = new NewParsedQuery();
    query.where = 'date > {Tomorrow}';

    const result = buildNewNotesQuery(query, 1, await newNotuCache());

    expect(result).toContain(`date > (CURRENT_DATE + INTERVAL'1d')`);
});