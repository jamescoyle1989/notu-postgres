import { expect, test } from 'vitest';
import { NotuCache, ParsedAttr, ParsedQuery, ParsedTag } from 'notu';
import { buildNotesQuery } from '../src/PostgresQueryBuilder';
import { testCacheFetcher } from './TestHelpers';


async function newNotuCache(): Promise<NotuCache> {
    const cache = new NotuCache(testCacheFetcher());
    await cache.populate();
    return cache;
}


test('buildNotesQuery correctly processes empty query', async () => {
    const query = new ParsedQuery();

    expect(buildNotesQuery(query, 1, await newNotuCache()))
        .toBe('SELECT n.id, n.spaceId, n.text, n.date FROM Note n LEFT JOIN Tag t ON n.id = t.id WHERE n.spaceId = 1;');
});

test('buildNotesQuery correctly processes query with order clause', async () => {
    const query = new ParsedQuery();
    query.order = 'date';

    expect(buildNotesQuery(query, 1, await newNotuCache()))
        .toBe('SELECT n.id, n.spaceId, n.text, n.date FROM Note n LEFT JOIN Tag t ON n.id = t.id WHERE n.spaceId = 1 ORDER BY date;');
});

test('buildNotesQuery correctly processes query with self tag filter', async () => {
    const query = new ParsedQuery();
    query.where = '{tag0}';
    query.tags.push((() => {
        const tag = new ParsedTag();
        tag.name = 'Tag 1';
        tag.space = null;
        tag.searchDepth = 0;
        return tag;
    })());

    expect(buildNotesQuery(query, 1, await newNotuCache()))
        .toBe(
            'SELECT n.id, n.spaceId, n.text, n.date ' +
            'FROM Note n LEFT JOIN Tag t ON n.id = t.id ' +
            'WHERE n.spaceId = 1 AND (n.id = 1);'
        );
});

test('buildNotesQuery correctly processes query with child tag filter', async () => {
    const query = new ParsedQuery();
    query.where = '{tag0}';
    query.tags.push((() => {
        const tag = new ParsedTag();
        tag.name = 'Tag 1';
        tag.space = null;
        tag.searchDepth = 1;
        tag.includeOwner = false;
        tag.strictSearchDepth = true;
        return tag;
    })());

    expect(buildNotesQuery(query, 1, await newNotuCache()))
        .toBe(
            'SELECT n.id, n.spaceId, n.text, n.date ' +
            'FROM Note n LEFT JOIN Tag t ON n.id = t.id ' +
            'WHERE n.spaceId = 1 AND (EXISTS(SELECT 1 FROM NoteTag nt WHERE nt.noteId = n.id AND nt.tagId = 1));'
        );
});

test('buildNotesQuery correctly processes query with child tag filter', async () => {
    const query = new ParsedQuery();
    query.where = '{tag0}';
    query.tags.push((() => {
        const tag = new ParsedTag();
        tag.name = 'Tag 1';
        tag.space = null;
        tag.searchDepth = 1;
        tag.includeOwner = true;
        tag.strictSearchDepth = true;
        return tag;
    })());

    expect(buildNotesQuery(query, 1, await newNotuCache()))
        .toBe(
            'SELECT n.id, n.spaceId, n.text, n.date ' +
            'FROM Note n LEFT JOIN Tag t ON n.id = t.id ' +
            'WHERE n.spaceId = 1 AND ((n.id = 1 OR EXISTS(SELECT 1 FROM NoteTag nt WHERE nt.noteId = n.id AND nt.tagId = 1)));'
        );
});

test('buildNotesQuery can search for strict matches 2 relations deep', async () => {
    const query = new ParsedQuery();
    query.where = '{tag0}';
    query.tags.push((() => {
        const tag = new ParsedTag();
        tag.name = 'Tag 3';
        tag.space = null;
        tag.searchDepth = 2;
        tag.includeOwner = false;
        tag.strictSearchDepth = true;
        return tag;
    })());

    expect(buildNotesQuery(query, 1, await newNotuCache()))
        .toBe(
            'SELECT n.id, n.spaceId, n.text, n.date ' +
            'FROM Note n LEFT JOIN Tag t ON n.id = t.id ' +
            'WHERE n.spaceId = 1 AND (EXISTS(SELECT 1 FROM NoteTag nt1 INNER JOIN NoteTag nt2 ON nt2.noteId = nt1.tagId WHERE nt1.noteId = n.id AND nt2.tagId = 3));'
        );
});

test('buildNotesQuery correctly processes query with attr exists condition', async () => {
    const query = new ParsedQuery();
    query.where = '{attr0}';
    query.attrs.push((() => {
        const attr = new ParsedAttr();
        attr.name = 'Attr 2';
        attr.exists = true;
        return attr;
    })());

    expect(buildNotesQuery(query, 1, await newNotuCache()))
        .toBe(
            'SELECT n.id, n.spaceId, n.text, n.date ' +
            'FROM Note n LEFT JOIN Tag t ON n.id = t.id ' +
            'WHERE n.spaceId = 1 AND (EXISTS(SELECT 1 FROM NoteAttr na WHERE na.noteId = n.id AND na.attrId = 2));'
        );
});

test('buildNotesQuery correctly processes query with attr condition', async () => {
    const query = new ParsedQuery();
    query.where = `{attr0} = 'hello'`;
    query.attrs.push((() => {
        const attr = new ParsedAttr();
        attr.name = 'Attr 1';
        attr.exists = false;
        return attr;
    })());

    expect(buildNotesQuery(query, 1, await newNotuCache()))
        .toBe(
            'SELECT n.id, n.spaceId, n.text, n.date ' +
            'FROM Note n LEFT JOIN Tag t ON n.id = t.id ' +
            `WHERE n.spaceId = 1 AND (CAST((SELECT na.value FROM NoteAttr na WHERE na.noteId = n.id AND na.attrId = 1) AS TEXT) = 'hello');`
        );
});

test('buildNotesQuery correctly processes query with attr exists condition on specific tags', async () => {
    const query = new ParsedQuery();
    query.where = '{attr0}';
    query.attrs.push((() => {
        const attr = new ParsedAttr();
        attr.name = 'Attr 1';
        attr.exists = true;
        attr.tagNameFilters = [(() => {
            const tag = new ParsedTag();
            tag.name = 'Tag 1';
            return tag;
        })()]
        return attr;
    })());

    expect(buildNotesQuery(query, 1, await newNotuCache()))
        .toBe(
            'SELECT n.id, n.spaceId, n.text, n.date ' +
            'FROM Note n LEFT JOIN Tag t ON n.id = t.id ' +
            'WHERE n.spaceId = 1 AND (EXISTS(SELECT 1 FROM NoteAttr na WHERE na.noteId = n.id AND na.attrId = 1 AND na.tagId IN (1)));'
        );
});

test('buildNotesQuery correctly processes query with attr condition on specific tags', async () => {
    const query = new ParsedQuery();
    query.where = `{attr0} = 'hello'`;
    query.attrs.push((() => {
        const attr = new ParsedAttr();
        attr.name = 'Attr 1';
        attr.exists = false;
        attr.tagNameFilters = [(() => {
            const tag = new ParsedTag();
            tag.name = 'Tag 1';
            return tag;
        })()]
        return attr;
    })());

    expect(buildNotesQuery(query, 1, await newNotuCache()))
        .toBe(
            'SELECT n.id, n.spaceId, n.text, n.date ' +
            'FROM Note n LEFT JOIN Tag t ON n.id = t.id ' +
            `WHERE n.spaceId = 1 AND (CAST((SELECT na.value FROM NoteAttr na WHERE na.noteId = n.id AND na.attrId = 1 AND na.tagId IN (1)) AS TEXT) = 'hello');`
        );
});

test('buildNotesQuery can handle {Now}', async () => {
    const query = new ParsedQuery();
    query.where = 'date > {Now}';

    const result = buildNotesQuery(query, 1, await newNotuCache());

    expect(result).toContain('date > NOW()');
});

test('buildNotesQuery can handle {Today}', async () => {
    const query = new ParsedQuery();
    query.where = 'date > {Today}';

    const result = buildNotesQuery(query, 1, await newNotuCache());

    expect(result).toContain(`date > CURRENT_DATE`);
});

test('buildNotesQuery can handle {Yesterday}', async () => {
    const query = new ParsedQuery();
    query.where = 'date > {Yesterday}';

    const result = buildNotesQuery(query, 1, await newNotuCache());

    expect(result).toContain(`date > (CURRENT_DATE - INTERVAL'1d')`);
});

test('buildNotesQuery can handle {Tomorrow}', async () => {
    const query = new ParsedQuery();
    query.where = 'date > {Tomorrow}';

    const result = buildNotesQuery(query, 1, await newNotuCache());

    expect(result).toContain(`date > (CURRENT_DATE + INTERVAL'1d')`);
});